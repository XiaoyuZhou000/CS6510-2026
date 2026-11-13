import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Everything the reconciler and the notification report read from one run directory (research N11):
 *
 * <pre>
 * DIR/subscribers/   snapshot.json, ready.json, receipts/*.jsonl, changes.jsonl  (or these directly in DIR)
 * DIR/exports/       sales.tsv, sale-events.tsv, selections.tsv, notifications.tsv, history.tsv, parked-events.tsv
 * DIR/run.json       run identity, written by the harness (optional)
 * DIR/backlog.tsv    backlog samples, written by the harness (optional)
 * </pre>
 *
 * The exports are {@code mysql --batch} output with a header row: tab-separated, {@code NULL} for SQL NULL, and
 * backslash escapes for tab, newline, and backslash. Times are epoch milliseconds.
 */
public final class RunData {
    public record SaleLine(String transactionId, long completedAtMs, String sku, int quantity) { }

    public record SaleEvent(String eventId, String aggregateKey, Long publishedAtMs, String saleId, Long completedAtMs,
                            String lines) { }

    public record Selection(String saleId, String saleEventId, long completedAtMs, String lines, long version,
                            long recipientCount, long notificationCount) { }

    public record Row(String notificationId, String saleId, String sku, String subscriberId, int quantity,
                      long completedAtMs, String state, long attempts, long chargedFailures, long unchargedFailures,
                      long operatorRetries, Long deliveredAtMs, Long parkedAtMs) { }

    public record History(long version, String subscriberId, String changeType, List<String> resultingSkus) { }

    public record ParkedEvent(String deadLetterId, String eventId, String topic, String reason) { }

    /** One receipt line; {@code undecodable} lines carry only the subscriber, offset, and receive time. */
    public record Receipt(String subscriberId, String notificationId, String saleId, String sku, int quantity,
                          long completedAtMs, long receivedAtMs, long channelOffset, boolean undecodable) { }

    public final Path dir;
    /** The subscriber folder: {@code DIR/subscribers} when it exists, else {@code DIR}. */
    public final Path subscribersDir;
    /** {@code snapshot.json} and {@code ready.json}; null when absent (a change-scenario run has neither). */
    public final RunFiles.Snapshot snapshot;
    public final RunFiles.Ready ready;
    /** The change driver's {@code changes.jsonl} records (T091), in file order; empty when absent. */
    public final List<Map<String, Object>> changes;
    public final List<SaleLine> sales;
    public final List<SaleEvent> saleEvents;
    public final List<Selection> selections;
    public final List<Row> rows;
    public final List<History> history;
    public final List<ParkedEvent> parkedEvents;
    public final List<Receipt> receipts;
    public final Map<String, Object> run;
    public final List<Map<String, String>> backlog;

    @SuppressWarnings("unchecked")
    private RunData(Path dir) throws IOException {
        this.dir = dir;
        Path subscribers = Files.isDirectory(dir.resolve("subscribers")) ? dir.resolve("subscribers") : dir;
        subscribersDir = subscribers;
        snapshot = Files.exists(subscribers.resolve("snapshot.json")) ? RunFiles.readSnapshot(subscribers) : null;
        ready = Files.exists(subscribers.resolve("ready.json")) ? RunFiles.readReady(subscribers) : null;
        changes = new ArrayList<>();
        Path changesFile = subscribers.resolve("changes.jsonl");
        if (Files.exists(changesFile)) {
            for (String line : Files.readAllLines(changesFile, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) changes.add(Json.parseObject(line));
            }
        }
        Path exports = dir.resolve("exports");

        sales = new ArrayList<>();
        for (Map<String, String> r : readTsv(exports.resolve("sales.tsv"))) {
            sales.add(new SaleLine(r.get("transaction_id"), num(r, "completed_at_ms"), r.get("sku"), (int) num(r, "quantity")));
        }
        saleEvents = new ArrayList<>();
        for (Map<String, String> r : readTsv(exports.resolve("sale-events.tsv"))) {
            saleEvents.add(new SaleEvent(r.get("event_id"), r.get("aggregate_key"), optional(r, "published_at_ms"),
                    r.get("sale_id"), optional(r, "completed_at_ms"), r.get("lines")));
        }
        selections = new ArrayList<>();
        for (Map<String, String> r : readTsv(exports.resolve("selections.tsv"))) {
            selections.add(new Selection(r.get("sale_id"), r.get("sale_event_id"), num(r, "completed_at_ms"), r.get("lines"),
                    num(r, "subscription_version"), num(r, "recipient_count"), num(r, "notification_count")));
        }
        rows = new ArrayList<>();
        for (Map<String, String> r : readTsv(exports.resolve("notifications.tsv"))) {
            rows.add(new Row(r.get("notification_id"), r.get("sale_id"), r.get("sku"), r.get("subscriber_id"),
                    (int) num(r, "quantity"), num(r, "completed_at_ms"), r.get("state"), num(r, "attempt_count"),
                    num(r, "charged_failures"), num(r, "uncharged_failures"), num(r, "operator_retries"),
                    optional(r, "delivered_at_ms"), optional(r, "parked_at_ms")));
        }
        history = new ArrayList<>();
        for (Map<String, String> r : readTsv(exports.resolve("history.tsv"))) {
            history.add(new History(num(r, "version"), r.get("subscriber_id"), r.get("change_type"),
                    Json.strings((List<Object>) Json.parse(r.get("resulting_skus")))));
        }
        parkedEvents = new ArrayList<>();
        for (Map<String, String> r : readTsv(exports.resolve("parked-events.tsv"))) {
            parkedEvents.add(new ParkedEvent(r.get("dead_letter_id"), r.get("event_id"), r.get("topic"), r.get("reason")));
        }
        receipts = readReceipts(subscribers.resolve("receipts"));
        Path runFile = dir.resolve("run.json");
        run = Files.exists(runFile) ? Json.parseObject(Files.readString(runFile, StandardCharsets.UTF_8)) : null;
        Path backlogFile = dir.resolve("backlog.tsv");
        backlog = Files.exists(backlogFile) ? readTsv(backlogFile) : List.of();
    }

    public static RunData load(Path dir) throws IOException {
        return new RunData(dir.toAbsolutePath().normalize());
    }

    private static List<Receipt> readReceipts(Path receiptsDir) throws IOException {
        List<Receipt> result = new ArrayList<>();
        if (!Files.isDirectory(receiptsDir)) return result;
        List<Path> files;
        try (Stream<Path> listed = Files.list(receiptsDir)) {
            files = listed.filter(p -> p.getFileName().toString().endsWith(".jsonl")).sorted().toList();
        }
        for (Path file : files) {
            String fallbackId = file.getFileName().toString().replaceFirst("\\.jsonl$", "");
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line.isBlank()) continue;
                Map<String, Object> r;
                try {
                    r = Json.parseObject(line);
                } catch (RuntimeException corrupt) {
                    result.add(new Receipt(fallbackId, null, null, null, 0, 0, 0, -(i + 1), true));
                    continue;
                }
                String subscriber = r.containsKey("subscriberId") ? Json.getString(r, "subscriberId") : fallbackId;
                if (Json.getBoolean(r, "undecodable")) {
                    result.add(new Receipt(subscriber, null, null, null, 0, 0, Json.getLong(r, "receivedAtMs"),
                            Json.getLong(r, "channelOffset"), true));
                } else {
                    result.add(new Receipt(subscriber, Json.getString(r, "notificationId"), Json.getString(r, "saleId"),
                            Json.getString(r, "sku"), (int) Json.getLong(r, "quantity"), Json.getLong(r, "completedAtMs"),
                            Json.getLong(r, "receivedAtMs"), Json.getLong(r, "channelOffset"), false));
                }
            }
        }
        return result;
    }

    /** A {@code mysql --batch} file with a header row; {@code NULL} becomes null. A missing file is an error. */
    public static List<Map<String, String>> readTsv(Path file) throws IOException {
        if (!Files.exists(file)) throw new IOException("Missing export " + file);
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        List<Map<String, String>> rows = new ArrayList<>();
        if (lines.isEmpty()) return rows;
        String[] header = stripBom(lines.get(0)).split("\t", -1);
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isEmpty()) continue;
            String[] fields = line.split("\t", -1);
            if (fields.length != header.length) {
                throw new IOException(file + " line " + (i + 1) + " has " + fields.length + " fields, expected " + header.length);
            }
            Map<String, String> row = new LinkedHashMap<>();
            for (int c = 0; c < header.length; c++) row.put(header[c], "NULL".equals(fields[c]) ? null : unescape(fields[c]));
            rows.add(row);
        }
        return rows;
    }

    private static String stripBom(String text) {
        return text.startsWith("﻿") ? text.substring(1) : text;
    }

    private static String unescape(String field) {
        if (field.indexOf('\\') < 0) return field;
        StringBuilder text = new StringBuilder(field.length());
        for (int i = 0; i < field.length(); i++) {
            char c = field.charAt(i);
            if (c != '\\' || i + 1 == field.length()) { text.append(c); continue; }
            char next = field.charAt(++i);
            switch (next) {
                case 't' -> text.append('\t');
                case 'n' -> text.append('\n');
                case 'r' -> text.append('\r');
                case '0' -> text.append('\0');
                case '\\' -> text.append('\\');
                default -> text.append('\\').append(next);
            }
        }
        return text.toString();
    }

    private static long num(Map<String, String> row, String column) {
        String value = row.get(column);
        if (value == null) throw new IllegalArgumentException("Column " + column + " is NULL or missing in " + row);
        return Long.parseLong(value.trim());
    }

    private static Long optional(Map<String, String> row, String column) {
        String value = row.get(column);
        return value == null || value.isBlank() ? null : Long.parseLong(value.trim());
    }

    /** Canonical {@code [{"sku","quantity"}]} lines as SKU -> quantity, or null when they cannot be parsed. */
    public static Map<String, Integer> parseLines(String json) {
        if (json == null) return null;
        try {
            Map<String, Integer> lines = new TreeMap<>();
            for (Object entry : (List<?>) Json.parse(json)) {
                Map<String, Object> line = Json.asObject(entry);
                lines.merge(Json.getString(line, "sku"), (int) Json.getLong(line, "quantity"), Integer::sum);
            }
            return lines;
        } catch (RuntimeException invalid) {
            return null;
        }
    }
}
