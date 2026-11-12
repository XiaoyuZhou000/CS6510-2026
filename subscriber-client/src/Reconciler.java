import java.util.ArrayList;
import java.util.HashMap;
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
 * {@code fixed} mode, owed = completed sales (per-SKU {@code accepted_scan} counts) x the subscriber snapshot.
 *
 * <p>Each failing key is reported in exactly one category:
 * <ul>
 *   <li>{@code saleEvents} (SC-001): a COMPLETED transaction with zero or several {@code SaleCompleted} rows, or with
 *       lines that differ from its sale; a sale event of no completed sale, or one that cannot be decoded.</li>
 *   <li>{@code fixedSubscriptions} (FR-023): a history version above {@code ready.json}'s, or a subscription state at
 *       that version that differs from {@code snapshot.json}.</li>
 *   <li>{@code selectionMismatch}: a completed sale with no selection or several, a selection whose version is not the
 *       ready version, whose lines differ from the sale, or that belongs to no completed sale.</li>
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
    public static final String SELECTION_MISMATCH = "selectionMismatch";
    public static final String MISSING_NOT_DELIVERED = "missingNotDelivered";
    public static final String MISSING_NOT_RECEIVED = "missingDeliveredNotReceived";
    public static final String UNEXPECTED = "unexpected";
    public static final String DUPLICATE = "duplicate";
    public static final String QUANTITY_MISMATCH = "quantityMismatch";
    public static final String PARKED = "parked";
    public static final List<String> CATEGORIES = List.of(SALE_EVENTS, FIXED_SUBSCRIPTIONS, SELECTION_MISMATCH,
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

    public static Result reconcile(RunData data, String mode) {
        if (HISTORY.equals(mode)) {
            throw new UnsupportedOperationException("--mode=history is not implemented yet (T092)");
        }
        if (!FIXED.equals(mode)) throw new IllegalArgumentException("--mode must be fixed or history: " + mode);

        Findings findings = new Findings();
        Map<String, Map<String, Integer>> sales = completedSales(data);
        long readyVersion = data.ready.subscriptionVersion();

        // Sale events (SC-001).
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
            if (!lines.equals(RunData.parseLines(event.lines())) || !("sale:" + sale).equals(event.aggregateKey())) {
                findings.add(SALE_EVENTS, sale);
            }
        });
        for (String sale : eventsBySale.keySet()) if (!sales.containsKey(sale)) findings.add(SALE_EVENTS, sale);

        // Fixed subscriptions (FR-023): no change after the ready version, and the state at it is the snapshot.
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

        // Notifications: rows and receipts against the owed set.
        Map<String, Owed> owed = owed(data);
        Map<String, List<RunData.Row>> rowsByKey = new HashMap<>();
        Map<String, Set<String>> keysById = new HashMap<>();
        for (RunData.Row row : data.rows) {
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
            String key = key(receipt.saleId(), receipt.sku(), receipt.subscriberId());
            receiptsByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(receipt);
            keysById.computeIfAbsent(receipt.notificationId(), id -> new TreeSet<>()).add(key);
            receiptsById.merge(receipt.notificationId(), 1, Integer::sum);
        }

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
        totals.put("readyVersion", readyVersion);
        totals.put("highestHistoryVersion", highestHistory);
        totals.put("subscribers", (long) data.snapshot.subscribers().size());
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
