import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Independent reconciliation of one run (FR-022-FR-026, research N11, contracts/subscriber-client-and-reconciliation.md
 * "ReconcileMain"). The owed set comes from Transaction's own records, never from the event under test: in
 * {@code fixed} mode, owed = completed sales (per-SKU {@code accepted_scan} counts) x the subscriber snapshot. In
 * {@code history} mode (scenario E, FR-027), owed = the recorded selections that equal the subscription history
 * replayed to their version, with the sales' {@code accepted_scan} quantities.
 *
 * <p>Each failing key is reported in exactly one category:
 * <ul>
 *   <li>{@code saleEvents} (SC-001): a COMPLETED transaction with zero or several {@code SaleCompleted} rows, or with
 *       lines or a completion time that differ from its sale; a sale event of no completed sale, or one that cannot be
 *       decoded.</li>
 *   <li>{@code fixedSubscriptions} (FR-023, fixed mode): a history version above {@code ready.json}'s, or a
 *       subscription state at that version that differs from {@code snapshot.json}.</li>
 *   <li>{@code history} (FR-007, FR-010, FR-027, history mode): a gap in the history versions, a sale selected below
 *       the version confirmed before its Complete, a confirmed change missing from the history, or a failed or
 *       rejected request's guarded subscriber that changed anyway ({@code changes.jsonl}).</li>
 *   <li>{@code selectionMismatch}: a completed sale with no selection or several, a selection whose lines differ from
 *       the sale or that belongs to no completed sale; in fixed mode one not at the ready version, in history mode one
 *       whose recipients differ from the replayed history (its keys are then not checked again below).</li>
 *   <li>{@code missingNotDelivered}: an owed key with no notification row, or only a {@code PENDING} one.</li>
 *   <li>{@code missingDeliveredNotReceived}: an owed key that is {@code DELIVERED} but has no receipt.</li>
 *   <li>{@code unexpected}: a row or receipt with no owed key, or a receipt that could not be decoded.</li>
 *   <li>{@code duplicate}: several rows or several received identities for one key, or one identity with two keys.</li>
 *   <li>{@code quantityMismatch}: a row's or receipt's quantity that differs from the owed quantity.</li>
 *   <li>{@code parked}: a {@code PARKED} notification (counted here rather than as missing) or a parked event.</li>
 * </ul>
 * Redeliveries, a notification identity received more than once, are counted in the totals and never fail.
 */
public final class Reconciler {
    public static final String FIXED = "fixed";
    public static final String HISTORY = "history";
    public static final int SAMPLES = 20;

    public static final String SALE_EVENTS = "saleEvents";
    public static final String FIXED_SUBSCRIPTIONS = "fixedSubscriptions";
    /** History mode's rule checks (FR-027): contiguity, causality, confirmed changes, and guarded subscribers. */
    public static final String HISTORY_RULE = "history";
    public static final String SELECTION_MISMATCH = "selectionMismatch";
    public static final String MISSING_NOT_DELIVERED = "missingNotDelivered";
    public static final String MISSING_NOT_RECEIVED = "missingDeliveredNotReceived";
    public static final String UNEXPECTED = "unexpected";
    public static final String DUPLICATE = "duplicate";
    public static final String QUANTITY_MISMATCH = "quantityMismatch";
    public static final String PARKED = "parked";
    public static final List<String> CATEGORIES = List.of(SALE_EVENTS, FIXED_SUBSCRIPTIONS, HISTORY_RULE, SELECTION_MISMATCH,
            MISSING_NOT_DELIVERED, MISSING_NOT_RECEIVED, UNEXPECTED, DUPLICATE, QUANTITY_MISMATCH, PARKED);

    /** One owed notification: a completed sale's SKU, sold {@code quantity} times, to one subscribed subscriber. */
    public record Owed(String saleId, String sku, String subscriberId, int quantity, long completedAtMs) {
        public String key() { return Reconciler.key(saleId, sku, subscriberId); }
    }

    public record Result(String verdict, String mode, Map<String, Long> counts, Map<String, Long> totals,
                         Map<String, List<String>> samples) {
        public boolean passed() { return "PASS".equals(verdict); }

        public Map<String, Object> toMap() {
            Map<String, Object> sampleMap = new LinkedHashMap<>();
            samples.forEach((category, keys) -> sampleMap.put(category, new ArrayList<Object>(keys)));
            return Json.object("verdict", verdict, "mode", mode, "counts", new LinkedHashMap<String, Object>(counts),
                    "totals", new LinkedHashMap<String, Object>(totals), "samples", sampleMap);
        }

        public String toJson() { return Json.pretty(toMap()); }
    }

    private Reconciler() { }

    public static String key(String saleId, String sku, String subscriberId) {
        return saleId + "/" + sku + "/" + subscriberId;
    }

    /** Completed sales from Transaction's export: transaction -> SKU -> quantity. */
    public static Map<String, Map<String, Integer>> completedSales(RunData data) {
        Map<String, Map<String, Integer>> sales = new TreeMap<>();
        for (RunData.SaleLine line : data.sales) {
            sales.computeIfAbsent(line.transactionId(), t -> new TreeMap<>()).merge(line.sku(), line.quantity(), Integer::sum);
        }
        return sales;
    }

    /** The owed set in fixed mode: every completed sale's SKUs x the snapshot's subscribers of each SKU. */
    public static Map<String, Owed> owed(RunData data) {
        Map<String, Set<String>> subscribersBySku = new HashMap<>();
        data.snapshot.subscribers().forEach((id, skus) -> {
            for (String sku : skus) subscribersBySku.computeIfAbsent(sku, s -> new TreeSet<>()).add(id);
        });
        Map<String, Long> completedAt = new HashMap<>();
        for (RunData.SaleLine line : data.sales) completedAt.put(line.transactionId(), line.completedAtMs());
        Map<String, Owed> owed = new LinkedHashMap<>();
        completedSales(data).forEach((sale, lines) -> lines.forEach((sku, quantity) -> {
            for (String subscriber : subscribersBySku.getOrDefault(sku, Set.of())) {
                Owed item = new Owed(sale, sku, subscriber, quantity, completedAt.get(sale));
                owed.put(item.key(), item);
            }
        }));
        return owed;
    }

    /** The owed set of {@code mode}: the snapshot in fixed mode, the verified selections in history mode. */
    public static Map<String, Owed> owed(RunData data, String mode) {
        if (HISTORY.equals(mode)) return history(data, completedSales(data), new Findings(), new HashSet<>());
        return owed(data);
    }

    public static Result reconcile(RunData data, String mode) throws IOException {
        if (!FIXED.equals(mode) && !HISTORY.equals(mode)) {
            throw new IllegalArgumentException("--mode must be fixed or history: " + mode);
        }
        boolean fixed = FIXED.equals(mode);
        if (fixed && (data.snapshot == null || data.ready == null)) {
            throw new IOException("fixed mode needs snapshot.json and ready.json in " + data.subscribersDir);
        }

        Findings findings = new Findings();
        Map<String, Map<String, Integer>> sales = completedSales(data);
        long readyVersion = fixed ? data.ready.subscriptionVersion() : -1;

        // Sale events (SC-001).
        Map<String, Long> completedAt = new HashMap<>();
        for (RunData.SaleLine line : data.sales) completedAt.put(line.transactionId(), line.completedAtMs());
        Map<String, List<RunData.SaleEvent>> eventsBySale = new TreeMap<>();
        for (RunData.SaleEvent event : data.saleEvents) {
            Map<String, Integer> lines = RunData.parseLines(event.lines());
            if (event.saleId() == null || event.saleId().isBlank() || lines == null) {
                findings.add(SALE_EVENTS, "event:" + event.eventId());
                continue;
            }
            eventsBySale.computeIfAbsent(event.saleId(), s -> new ArrayList<>()).add(event);
        }
        sales.forEach((sale, lines) -> {
            List<RunData.SaleEvent> events = eventsBySale.get(sale);
            if (events == null || events.size() != 1) {
                findings.add(SALE_EVENTS, sale);
                return;
            }
            RunData.SaleEvent event = events.get(0);
            // The event carries the sale's own completion instant, so the two times must agree to the millisecond;
            // this also catches an export that reads the DATETIME columns in the wrong time zone.
            if (!lines.equals(RunData.parseLines(event.lines())) || !("sale:" + sale).equals(event.aggregateKey())
                    || !completedAt.get(sale).equals(event.completedAtMs())) {
                findings.add(SALE_EVENTS, sale);
            }
        });
        for (String sale : eventsBySale.keySet()) if (!sales.containsKey(sale)) findings.add(SALE_EVENTS, sale);

        Map<String, Owed> owed;
        // Sales whose selection failed its check: reported once, and kept out of the delivery checks (history mode).
        Set<String> excluded = new HashSet<>();
        if (fixed) {
            fixedChecks(data, sales, readyVersion, findings);
            owed = owed(data);
        } else {
            owed = history(data, sales, findings, excluded);
        }

        // Notifications: rows and receipts against the owed set.
        Map<String, List<RunData.Row>> rowsByKey = new HashMap<>();
        Map<String, Set<String>> keysById = new HashMap<>();
        for (RunData.Row row : data.rows) {
            if (excluded.contains(row.saleId())) continue;
            String key = key(row.saleId(), row.sku(), row.subscriberId());
            rowsByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            keysById.computeIfAbsent(row.notificationId(), id -> new TreeSet<>()).add(key);
        }
        Map<String, List<RunData.Receipt>> receiptsByKey = new HashMap<>();
        Map<String, Integer> receiptsById = new HashMap<>();
        long validReceipts = 0;
        long undecodable = 0;
        for (RunData.Receipt receipt : data.receipts) {
            if (receipt.undecodable()) {
                undecodable++;
                findings.add(UNEXPECTED, "receipt:" + receipt.subscriberId() + "@" + receipt.channelOffset());
                continue;
            }
            validReceipts++;
            receiptsById.merge(receipt.notificationId(), 1, Integer::sum);
            if (excluded.contains(receipt.saleId())) continue;
            String key = key(receipt.saleId(), receipt.sku(), receipt.subscriberId());
            receiptsByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(receipt);
            keysById.computeIfAbsent(receipt.notificationId(), id -> new TreeSet<>()).add(key);
        }
        return deliveryChecks(data, mode, sales, owed, rowsByKey, keysById, receiptsByKey, receiptsById, validReceipts,
                undecodable, readyVersion, findings);
    }

    /** Fixed mode (FR-023): no change after the ready version, the state at it is the snapshot, one selection per sale. */
    private static void fixedChecks(RunData data, Map<String, Map<String, Integer>> sales, long readyVersion,
                                    Findings findings) {
        Map<String, List<String>> state = new TreeMap<>();
        List<RunData.History> ordered = new ArrayList<>(data.history);
        ordered.sort((a, b) -> Long.compare(a.version(), b.version()));
        for (RunData.History change : ordered) {
            if (change.version() > readyVersion) {
                findings.add(FIXED_SUBSCRIPTIONS, "version:" + change.version());
                continue;
            }
            List<String> resulting = new ArrayList<>(new TreeSet<>(change.resultingSkus()));
            if ("CANCEL".equals(change.changeType()) || resulting.isEmpty()) state.remove(change.subscriberId());
            else state.put(change.subscriberId(), resulting);
        }
        Set<String> subscribers = new TreeSet<>(state.keySet());
        subscribers.addAll(data.snapshot.subscribers().keySet());
        for (String subscriber : subscribers) {
            List<String> snapshotSkus = data.snapshot.subscribers().get(subscriber);
            List<String> expected = snapshotSkus == null ? null : new ArrayList<>(new TreeSet<>(snapshotSkus));
            if (!Objects.equals(expected, state.get(subscriber))) findings.add(FIXED_SUBSCRIPTIONS, subscriber);
        }

        // Selections: one per completed sale, at the ready version, with the sale's lines.
        Map<String, List<RunData.Selection>> selectionsBySale = new TreeMap<>();
        for (RunData.Selection selection : data.selections) {
            selectionsBySale.computeIfAbsent(selection.saleId(), s -> new ArrayList<>()).add(selection);
        }
        sales.forEach((sale, lines) -> {
            List<RunData.Selection> selected = selectionsBySale.get(sale);
            if (selected == null || selected.size() != 1) {
                findings.add(SELECTION_MISMATCH, sale);
                return;
            }
            RunData.Selection selection = selected.get(0);
            if (selection.version() != readyVersion || !lines.equals(RunData.parseLines(selection.lines()))) {
                findings.add(SELECTION_MISMATCH, sale);
            }
        });
        for (String sale : selectionsBySale.keySet()) if (!sales.containsKey(sale)) findings.add(SELECTION_MISMATCH, sale);
    }

    /**
     * History mode (FR-027, research N12). Returns the owed set: the recorded selections that equal the history
     * replayed to their version. Checks, in its own categories:
     * <ul>
     *   <li>{@code history}: versions 1..N without a gap ({@code version:v}); a sale selected below the version
     *       confirmed before its Complete ({@code causality:t}); a confirmed change with no history row at its version
     *       ({@code confirmed:s@v}); a guarded subscriber, the target of a failed or rejected request, changed at a
     *       version it was not allowed ({@code guard:s@v});</li>
     *   <li>{@code selectionMismatch}: no selection or several, other lines, a version past the history, or
     *       recipients that differ from the replay. Such a sale is added to {@code excluded}, so its rows and receipts
     *       are not reported again as missing or unexpected.</li>
     * </ul>
     */
    private static Map<String, Owed> history(RunData data, Map<String, Map<String, Integer>> sales, Findings findings,
                                             Set<String> excluded) {
        List<RunData.History> ordered = new ArrayList<>(data.history);
        ordered.sort((a, b) -> Long.compare(a.version(), b.version()));
        long next = 1;
        for (RunData.History change : ordered) {
            if (change.version() < next) findings.add(HISTORY_RULE, "version:" + change.version());
            for (long missing = next; missing < change.version(); missing++) findings.add(HISTORY_RULE, "version:" + missing);
            next = Math.max(next, change.version() + 1);
        }
        long highest = ordered.isEmpty() ? 0 : ordered.get(ordered.size() - 1).version();

        Map<String, List<RunData.Selection>> selectionsBySale = new TreeMap<>();
        for (RunData.Selection selection : data.selections) {
            selectionsBySale.computeIfAbsent(selection.saleId(), s -> new ArrayList<>()).add(selection);
        }
        for (String sale : selectionsBySale.keySet()) {
            if (!sales.containsKey(sale)) {
                findings.add(SELECTION_MISMATCH, sale);
                excluded.add(sale);
            }
        }
        Map<String, Long> completedAt = new HashMap<>();
        for (RunData.SaleLine line : data.sales) completedAt.put(line.transactionId(), line.completedAtMs());
        Map<String, Set<String>> recordedBySale = new HashMap<>();
        for (RunData.Row row : data.rows) {
            recordedBySale.computeIfAbsent(row.saleId(), s -> new TreeSet<>()).add(key(row.saleId(), row.sku(), row.subscriberId()));
        }

        // Each sale's single selection, in version order, against the history replayed to that version.
        List<RunData.Selection> checked = new ArrayList<>();
        for (Map.Entry<String, Map<String, Integer>> sale : sales.entrySet()) {
            List<RunData.Selection> selected = selectionsBySale.get(sale.getKey());
            if (selected == null || selected.size() != 1 || !sale.getValue().equals(RunData.parseLines(selected.get(0).lines()))
                    || selected.get(0).version() > highest || selected.get(0).version() < 0) {
                findings.add(SELECTION_MISMATCH, sale.getKey());
                excluded.add(sale.getKey());
                continue;
            }
            checked.add(selected.get(0));
        }
        checked.sort((a, b) -> Long.compare(a.version(), b.version()));
        Map<String, Owed> owed = new LinkedHashMap<>();
        Map<String, Set<String>> state = new HashMap<>();
        Map<String, Set<String>> subscribersBySku = new HashMap<>();
        int applied = 0;
        for (RunData.Selection selection : checked) {
            while (applied < ordered.size() && ordered.get(applied).version() <= selection.version()) {
                RunData.History change = ordered.get(applied++);
                Set<String> before = state.remove(change.subscriberId());
                if (before != null) for (String sku : before) subscribersBySku.getOrDefault(sku, new TreeSet<>()).remove(change.subscriberId());
                if (!"CANCEL".equals(change.changeType()) && !change.resultingSkus().isEmpty()) {
                    Set<String> after = new TreeSet<>(change.resultingSkus());
                    state.put(change.subscriberId(), after);
                    for (String sku : after) subscribersBySku.computeIfAbsent(sku, s -> new TreeSet<>()).add(change.subscriberId());
                }
            }
            String sale = selection.saleId();
            Map<String, Owed> expected = new LinkedHashMap<>();
            Set<String> recipients = new TreeSet<>();
            sales.get(sale).forEach((sku, quantity) -> {
                for (String subscriber : subscribersBySku.getOrDefault(sku, Set.of())) {
                    Owed item = new Owed(sale, sku, subscriber, quantity, completedAt.get(sale));
                    expected.put(item.key(), item);
                    recipients.add(subscriber);
                }
            });
            if (!expected.keySet().equals(recordedBySale.getOrDefault(sale, Set.of()))
                    || selection.recipientCount() != recipients.size()
                    || selection.notificationCount() != expected.size()) {
                findings.add(SELECTION_MISMATCH, sale);
                excluded.add(sale);
                continue;
            }
            owed.putAll(expected);
        }

        // The change driver's records (changes.jsonl).
        Map<Long, String> subscriberAt = new HashMap<>();
        for (RunData.History change : ordered) subscriberAt.putIfAbsent(change.version(), change.subscriberId());
        for (Map<String, Object> record : data.changes) {
            String kind = Json.getString(record, "kind");
            if ("sale".equals(kind)) {
                String transaction = Json.getString(record, "transactionId");
                long confirmed = Json.getLong(record, "confirmedVersion");
                List<RunData.Selection> selected = selectionsBySale.get(transaction);
                if (!sales.containsKey(transaction)) findings.add(HISTORY_RULE, "sale:" + transaction);
                else if (selected != null && selected.size() == 1 && selected.get(0).version() < confirmed) {
                    findings.add(HISTORY_RULE, "causality:" + transaction);
                }
            } else if ("change".equals(kind)) {
                Object status = record.get("status");
                Object version = record.get("version");
                if (status instanceof Long code && code >= 200 && code < 300 && version instanceof Long confirmed) {
                    String subscriber = Json.getString(record, "subscriberId");
                    if (!subscriber.equals(subscriberAt.get(confirmed))) {
                        findings.add(HISTORY_RULE, "confirmed:" + subscriber + "@" + confirmed);
                    }
                }
            } else if ("guard".equals(kind)) {
                String subscriber = Json.getString(record, "subscriberId");
                Set<Long> allowed = new HashSet<>();
                for (Object version : Json.getList(record, "allowedVersions")) allowed.add(((Number) version).longValue());
                for (RunData.History change : ordered) {
                    if (change.subscriberId().equals(subscriber) && !allowed.contains(change.version())) {
                        findings.add(HISTORY_RULE, "guard:" + subscriber + "@" + change.version());
                    }
                }
            }
        }
        return owed;
    }

    private static Result deliveryChecks(RunData data, String mode, Map<String, Map<String, Integer>> sales,
                                         Map<String, Owed> owed, Map<String, List<RunData.Row>> rowsByKey,
                                         Map<String, Set<String>> keysById, Map<String, List<RunData.Receipt>> receiptsByKey,
                                         Map<String, Integer> receiptsById, long validReceipts, long undecodable,
                                         long readyVersion, Findings findings) {
        long delivered = 0;
        long received = 0;
        for (Owed item : owed.values()) {
            String key = item.key();
            List<RunData.Row> rows = rowsByKey.getOrDefault(key, List.of());
            List<RunData.Receipt> receipts = receiptsByKey.getOrDefault(key, List.of());
            if (rows.size() > 1) findings.add(DUPLICATE, key);
            Set<String> receivedIds = new TreeSet<>();
            for (RunData.Receipt receipt : receipts) receivedIds.add(receipt.notificationId());
            if (receivedIds.size() > 1) findings.add(DUPLICATE, key);
            for (RunData.Row row : rows) if (row.quantity() != item.quantity()) findings.add(QUANTITY_MISMATCH, key);
            for (RunData.Receipt receipt : receipts) {
                if (receipt.quantity() != item.quantity()) findings.add(QUANTITY_MISMATCH, key);
            }
            boolean isDelivered = rows.stream().anyMatch(r -> "DELIVERED".equals(r.state()));
            boolean isParked = rows.stream().anyMatch(r -> "PARKED".equals(r.state()));
            if (isDelivered) delivered++;
            if (!receipts.isEmpty()) received++;
            if (isParked) findings.add(PARKED, key);
            else if (!isDelivered) findings.add(MISSING_NOT_DELIVERED, key);
            else if (receipts.isEmpty()) findings.add(MISSING_NOT_RECEIVED, key);
        }
        for (String key : new TreeSet<>(rowsByKey.keySet())) if (!owed.containsKey(key)) findings.add(UNEXPECTED, key);
        for (String key : new TreeSet<>(receiptsByKey.keySet())) if (!owed.containsKey(key)) findings.add(UNEXPECTED, key);
        for (Map.Entry<String, Set<String>> identity : new TreeMap<>(keysById).entrySet()) {
            if (identity.getValue().size() > 1) findings.add(DUPLICATE, "id:" + identity.getKey());
        }
        for (RunData.ParkedEvent event : data.parkedEvents) {
            findings.add(PARKED, "event:" + (event.eventId() == null ? "dead-letter-" + event.deadLetterId() : event.eventId()));
        }

        long redeliveries = 0;
        for (int count : receiptsById.values()) redeliveries += count - 1;
        long parkedRows = data.rows.stream().filter(r -> "PARKED".equals(r.state())).count();
        long highestHistory = data.history.stream().mapToLong(RunData.History::version).max().orElse(0);

        Map<String, Long> totals = new LinkedHashMap<>();
        totals.put("completedSales", (long) sales.size());
        totals.put("saleEvents", (long) data.saleEvents.size());
        totals.put("selections", (long) data.selections.size());
        if (data.ready != null && FIXED.equals(mode)) totals.put("readyVersion", readyVersion);
        totals.put("highestHistoryVersion", highestHistory);
        totals.put("subscribers", subscriberCount(data));
        if (HISTORY.equals(mode)) {
            totals.put("changeRecords", data.changes.stream().filter(r -> "change".equals(r.get("kind"))).count());
            totals.put("confirmedBeforeSales", data.changes.stream().filter(r -> "sale".equals(r.get("kind"))).count());
            totals.put("guardedSubscribers", data.changes.stream().filter(r -> "guard".equals(r.get("kind"))).count());
        }
        totals.put("owed", (long) owed.size());
        totals.put("notificationRows", (long) data.rows.size());
        totals.put("delivered", delivered);
        totals.put("parkedNotifications", parkedRows);
        totals.put("receipts", validReceipts);
        totals.put("received", received);
        totals.put("redeliveries", redeliveries);
        totals.put("undecodableReceipts", undecodable);
        totals.put("parkedEvents", (long) data.parkedEvents.size());
        return findings.result(mode, totals);
    }

    /** The snapshot's subscribers, or, without a snapshot, every subscriber the history names. */
    public static long subscriberCount(RunData data) {
        if (data.snapshot != null) return data.snapshot.subscribers().size();
        return data.history.stream().map(RunData.History::subscriberId).distinct().count();
    }

    /** Failing keys per category, each key at most once per category; samples are the first 20 keys. */
    private static final class Findings {
        private final Map<String, LinkedHashSet<String>> keys = new LinkedHashMap<>();

        Findings() {
            for (String category : CATEGORIES) keys.put(category, new LinkedHashSet<>());
        }

        void add(String category, String key) {
            keys.get(category).add(key);
        }

        Result result(String mode, Map<String, Long> totals) {
            Map<String, Long> counts = new LinkedHashMap<>();
            Map<String, List<String>> samples = new LinkedHashMap<>();
            boolean pass = true;
            for (String category : CATEGORIES) {
                Set<String> failing = keys.get(category);
                counts.put(category, (long) failing.size());
                samples.put(category, failing.stream().limit(SAMPLES).toList());
                if (!failing.isEmpty()) pass = false;
            }
            return new Result(pass ? "PASS" : "FAIL", mode, counts, totals, samples);
        }
    }
}
