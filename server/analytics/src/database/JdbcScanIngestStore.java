package database;

import analytics.ProductNameMap;
import metrics.LatencyHistogram;
import metrics.PerfMetrics;
import faults.FaultHooks;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

/**
 * Ingestion with position assignment (contracts/scan-event-delivery.md section 3): at most six statements
 * and one commit per batch, whatever its size (R22).
 *
 * <ol>
 *   <li>{@code SELECT ... FOR UPDATE} of the {@code scan_sequence} singleton, which serializes batches
 *       across every Analytics instance;</li>
 *   <li>one tuple-IN SELECT of the batch's identities that are already stored;</li>
 *   <li>one batched multi-row INSERT of the new events, at {@code last_position + 1 ...} in batch order;</li>
 *   <li>one upsert of the names the {@link ProductNameMap} lacks or holds differently, only when there are any;</li>
 *   <li>the {@code scan_sequence} UPDATE;</li>
 *   <li>the {@code consumer_offset} upsert (FR-049), also for a batch of duplicates, then the commit.</li>
 * </ol>
 *
 * <p>The stored offset is overwritten, not maximized: after a recreated topic the consumer reads from the
 * earliest offset again, and the smaller offsets it stores are the ones a restart must resume from.</p>
 *
 * <p>The name map changes only after the commit returned. Test-only hooks
 * {@code analytics.ingest.beforeCommit} and {@code analytics.ingest.afterCommit} surround the commit.</p>
 */
public final class JdbcScanIngestStore implements ScanIngestStore {
    private static final String LOCK_SEQUENCE =
            "SELECT last_position FROM scan_sequence WHERE sequence_key = 1 FOR UPDATE";
    private static final String INSERT_EVENT = "INSERT INTO accepted_scan "
            + "(position, source_transaction_id, source_scan_sequence, sku) VALUES (?, ?, ?, ?)";
    private static final String ADVANCE_SEQUENCE = "UPDATE scan_sequence SET last_position = ? WHERE sequence_key = 1";
    private static final String STORE_OFFSET = "INSERT INTO consumer_offset (topic, partition_no, next_offset, updated_at) "
            + "VALUES (?, ?, ?, NOW(3)) AS incoming ON DUPLICATE KEY UPDATE next_offset = incoming.next_offset, "
            + "updated_at = incoming.updated_at";
    private static final String READ_OFFSET =
            "SELECT next_offset FROM consumer_offset WHERE topic = ? AND partition_no = ?";
    private static final LatencyHistogram BATCH = PerfMetrics.timer("analytics.ingest.batch");
    private static final LongAdder STORED = PerfMetrics.counter("analytics.ingest.stored");
    private static final LongAdder DUPLICATES = PerfMetrics.counter("analytics.ingest.duplicates");
    private static final LongAdder NAME_WRITES = PerfMetrics.counter("analytics.ingest.nameWrites");

    private final ConnectionPool pool;
    private final ProductNameMap names;

    public JdbcScanIngestStore(ConnectionPool pool, ProductNameMap names) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.names = Objects.requireNonNull(names, "names");
    }

    @Override
    public IngestResult ingest(List<ScanEvent> events, ConsumedOffset next) {
        Objects.requireNonNull(events, "events");
        Objects.requireNonNull(next, "next");
        if (events.isEmpty() || events.size() > MAX_BATCH) {
            throw new IllegalArgumentException("A batch holds 1 to " + MAX_BATCH + " events");
        }
        long started = System.nanoTime();
        // First occurrence of each identity, in batch order: a repeat inside the batch is a duplicate.
        Map<Identity, ScanEvent> unique = new LinkedHashMap<>();
        for (ScanEvent event : events) unique.putIfAbsent(event.identity(), event);

        Committed committed = JdbcUnitOfWork.inContendedTransaction(pool, "analytics.ingest", connection -> {
            long last = lockSequence(connection);
            Set<Identity> stored = storedIdentities(connection, unique.keySet());
            List<ScanEvent> fresh = new ArrayList<>(unique.size());
            for (ScanEvent event : unique.values()) {
                if (!stored.contains(event.identity())) fresh.add(event);
            }
            if (fresh.isEmpty()) {
                storeOffset(connection, next);
                FaultHooks.hit("analytics.ingest.beforeCommit");
                return new Committed(0, 0, 0, Map.of());
            }
            insertEvents(connection, fresh, last + 1);
            Map<String, String> changedNames = names.diff(fresh);
            if (!changedNames.isEmpty()) upsertNames(connection, changedNames);
            advanceSequence(connection, last + fresh.size());
            storeOffset(connection, next);
            FaultHooks.hit("analytics.ingest.beforeCommit");
            return new Committed(fresh.size(), last + 1, last + fresh.size(), changedNames);
        });

        names.apply(committed.names());
        int duplicates = events.size() - committed.stored();
        STORED.add(committed.stored());
        DUPLICATES.add(duplicates);
        NAME_WRITES.add(committed.names().size());
        BATCH.recordSince(started);
        FaultHooks.hit("analytics.ingest.afterCommit");
        return new IngestResult(committed.stored(), duplicates, committed.first(), committed.last());
    }

    @Override
    public OptionalLong readNextOffset(String topic, int partition) {
        return JdbcUnitOfWork.read(pool, "analytics.offset.read", connection -> {
            try (PreparedStatement select = connection.prepareStatement(READ_OFFSET)) {
                select.setString(1, topic);
                select.setInt(2, partition);
                try (ResultSet row = select.executeQuery()) {
                    return row.next() ? OptionalLong.of(row.getLong(1)) : OptionalLong.empty();
                }
            }
        });
    }

    @Override
    public Map<String, String> readProductNames(int limit) {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        return JdbcUnitOfWork.read(pool, "analytics.names.load", connection -> {
            Map<String, String> stored = new LinkedHashMap<>();
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT sku, name FROM product_name ORDER BY sku LIMIT ?")) {
                select.setInt(1, limit);
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) stored.put(rows.getString(1), rows.getString(2));
                }
            }
            return stored;
        });
    }

    private static long lockSequence(Connection connection) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(LOCK_SEQUENCE);
             ResultSet row = select.executeQuery()) {
            if (!row.next()) throw new SQLException("scan_sequence singleton is missing");
            return row.getLong(1);
        }
    }

    private static Set<Identity> storedIdentities(Connection connection, Set<Identity> batch) throws SQLException {
        StringBuilder sql = new StringBuilder(96 + batch.size() * 8)
                .append("SELECT source_transaction_id, source_scan_sequence FROM accepted_scan ")
                .append("WHERE (source_transaction_id, source_scan_sequence) IN (");
        for (int i = 0; i < batch.size(); i++) sql.append(i == 0 ? "(?, ?)" : ", (?, ?)");
        sql.append(')');
        Set<Identity> stored = new HashSet<>();
        try (PreparedStatement select = connection.prepareStatement(sql.toString())) {
            int parameter = 1;
            for (Identity identity : batch) {
                select.setString(parameter++, identity.transactionId());
                select.setInt(parameter++, identity.scanSequence());
            }
            try (ResultSet rows = select.executeQuery()) {
                while (rows.next()) stored.add(new Identity(rows.getString(1), rows.getInt(2)));
            }
        }
        return stored;
    }

    /** One batched INSERT; {@code rewriteBatchedStatements} (R20) sends it as one multi-row statement. */
    private static void insertEvents(Connection connection, List<ScanEvent> fresh, long firstPosition)
            throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement(INSERT_EVENT)) {
            long position = firstPosition;
            for (ScanEvent event : fresh) {
                insert.setLong(1, position++);
                insert.setString(2, event.transactionId());
                insert.setInt(3, event.scanSequence());
                insert.setString(4, event.sku());
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    private static void upsertNames(Connection connection, Map<String, String> changed) throws SQLException {
        StringBuilder sql = new StringBuilder(96 + changed.size() * 8).append("INSERT INTO product_name (sku, name) VALUES ");
        for (int i = 0; i < changed.size(); i++) sql.append(i == 0 ? "(?, ?)" : ", (?, ?)");
        sql.append(" AS incoming ON DUPLICATE KEY UPDATE name = incoming.name");
        try (PreparedStatement upsert = connection.prepareStatement(sql.toString())) {
            int parameter = 1;
            for (Map.Entry<String, String> name : changed.entrySet()) {
                upsert.setString(parameter++, name.getKey());
                upsert.setString(parameter++, name.getValue());
            }
            upsert.executeUpdate();
        }
    }

    private static void advanceSequence(Connection connection, long lastPosition) throws SQLException {
        try (PreparedStatement update = connection.prepareStatement(ADVANCE_SEQUENCE)) {
            update.setLong(1, lastPosition);
            if (update.executeUpdate() != 1) throw new SQLException("scan_sequence singleton is missing");
        }
    }

    private static void storeOffset(Connection connection, ConsumedOffset next) throws SQLException {
        try (PreparedStatement upsert = connection.prepareStatement(STORE_OFFSET)) {
            upsert.setString(1, next.topic());
            upsert.setInt(2, next.partition());
            upsert.setLong(3, next.nextOffset());
            upsert.executeUpdate();
        }
    }

    private record Committed(int stored, long first, long last, Map<String, String> names) { }
}
