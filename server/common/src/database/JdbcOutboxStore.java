package database;

import events.OutboxRow;
import events.OutboxStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * {@code outbox_event} on the owning service's own pool (D5; 006 Implementation Rule 5). The table has the
 * same shape in the Transaction and Inventory schemas, so one adapter serves both, instantiated with each
 * service's pool.
 *
 * <ul>
 *   <li>{@link #insert} joins the caller's open {@link JdbcUnitOfWork} through its connection; database
 *       adapters that need the unit's deadlock retries call {@link #insertRow}, or {@link #insertRows} for a
 *       batch such as the catalog publication, inside their own work.</li>
 *   <li>Rows are only ever updated in {@code published_at} and {@code send_count}, each in one statement and
 *       one commit per batch. {@code published_at} keeps the first confirmation time.</li>
 *   <li>The sweep query uses the {@code (published_at, outbox_seq)} index.</li>
 * </ul>
 */
public final class JdbcOutboxStore implements OutboxStore<Connection> {
    private static final String COLUMNS = "event_id, topic, routing_key, event_type, aggregate_key, envelope, created_at";
    private static final String INSERT = "INSERT INTO outbox_event (" + COLUMNS + ") VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String UNPUBLISHED = "SELECT " + COLUMNS + " FROM outbox_event "
            + "WHERE published_at IS NULL AND created_at < ? ORDER BY outbox_seq LIMIT ?";
    private static final int IN_LIST_CHUNK = 500;

    private final ConnectionPool pool;
    private final String metricPrefix;

    /** {@code metricPrefix} names the timed units, for example {@code transaction.outbox}. */
    public JdbcOutboxStore(ConnectionPool pool, String metricPrefix) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.metricPrefix = Objects.requireNonNull(metricPrefix, "metricPrefix");
    }

    @Override
    public void insert(Connection unitOfWork, OutboxRow row) {
        try {
            insertRow(unitOfWork, row);
        } catch (SQLException failure) {
            throw new StoreFailure("Outbox insert failed", failure);
        }
    }

    /** The INSERT on the caller's connection, leaving commit or rollback to the caller's unit. */
    public static void insertRow(Connection connection, OutboxRow row) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
            insert.setString(1, row.eventId());
            insert.setString(2, row.topic());
            insert.setString(3, row.routingKey());
            insert.setString(4, row.eventType());
            insert.setString(5, row.aggregateKey());
            insert.setBytes(6, row.envelope());
            insert.setTimestamp(7, Timestamp.from(row.createdAt()));
            insert.executeUpdate();
        }
    }

    /**
     * One batched INSERT of {@code rows} on the caller's connection, in the order given, so their
     * {@code outbox_seq} follows that order; the driver rewrites it into multi-row statements.
     */
    public static void insertRows(Connection connection, List<OutboxRow> rows) throws SQLException {
        if (rows.isEmpty()) return;
        try (PreparedStatement insert = connection.prepareStatement(INSERT)) {
            for (OutboxRow row : rows) {
                insert.setString(1, row.eventId());
                insert.setString(2, row.topic());
                insert.setString(3, row.routingKey());
                insert.setString(4, row.eventType());
                insert.setString(5, row.aggregateKey());
                insert.setBytes(6, row.envelope());
                insert.setTimestamp(7, Timestamp.from(row.createdAt()));
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    @Override
    public void markPublished(Collection<String> eventIds, Instant publishedAt) {
        if (eventIds.isEmpty()) return;
        Objects.requireNonNull(publishedAt, "publishedAt");
        List<String> ids = List.copyOf(eventIds);
        JdbcUnitOfWork.inTransaction(pool, metricPrefix + ".mark", connection -> {
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE outbox_event SET published_at = ? WHERE published_at IS NULL AND event_id IN ("
                            + placeholders(ids.size()) + ")")) {
                update.setTimestamp(1, Timestamp.from(publishedAt));
                for (int i = 0; i < ids.size(); i++) update.setString(i + 2, ids.get(i));
                return update.executeUpdate();
            }
        });
    }

    @Override
    public List<OutboxRow> selectUnpublished(Instant olderThan, int limit) {
        return JdbcUnitOfWork.read(pool, metricPrefix + ".sweep", connection -> {
            try (PreparedStatement select = connection.prepareStatement(UNPUBLISHED)) {
                select.setTimestamp(1, Timestamp.from(olderThan));
                select.setInt(2, limit);
                return rows(select);
            }
        });
    }

    @Override
    public Optional<OutboxRow> findByEventId(String eventId) {
        List<OutboxRow> rows = findByEventIds(List.of(eventId));
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    @Override
    public List<OutboxRow> findByEventIds(Collection<String> eventIds) {
        if (eventIds.isEmpty()) return List.of();
        List<String> ids = List.copyOf(eventIds);
        return JdbcUnitOfWork.read(pool, metricPrefix + ".load", connection -> {
            List<OutboxRow> found = new ArrayList<>(ids.size());
            for (int from = 0; from < ids.size(); from += IN_LIST_CHUNK) {
                List<String> chunk = ids.subList(from, Math.min(ids.size(), from + IN_LIST_CHUNK));
                try (PreparedStatement select = connection.prepareStatement("SELECT " + COLUMNS
                        + " FROM outbox_event WHERE event_id IN (" + placeholders(chunk.size()) + ")")) {
                    for (int i = 0; i < chunk.size(); i++) select.setString(i + 1, chunk.get(i));
                    found.addAll(rows(select));
                }
            }
            return found;
        });
    }

    @Override
    public List<OutboxRow> findByAggregateKey(String aggregateKey) {
        return JdbcUnitOfWork.read(pool, metricPrefix + ".load", connection -> {
            try (PreparedStatement select = connection.prepareStatement("SELECT " + COLUMNS
                    + " FROM outbox_event WHERE aggregate_key = ? ORDER BY outbox_seq")) {
                select.setString(1, aggregateKey);
                return rows(select);
            }
        });
    }

    @Override
    public void incrementSendCount(Collection<String> eventIds) {
        if (eventIds.isEmpty()) return;
        List<String> ids = List.copyOf(eventIds);
        JdbcUnitOfWork.inTransaction(pool, metricPrefix + ".resend", connection -> {
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE outbox_event SET send_count = send_count + 1 WHERE event_id IN ("
                            + placeholders(ids.size()) + ")")) {
                for (int i = 0; i < ids.size(); i++) update.setString(i + 1, ids.get(i));
                return update.executeUpdate();
            }
        });
    }

    @Override
    public long countUnpublished() {
        return JdbcUnitOfWork.read(pool, metricPrefix + ".count", connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT COUNT(*) FROM outbox_event WHERE published_at IS NULL");
                 ResultSet row = select.executeQuery()) {
                row.next();
                return row.getLong(1);
            }
        });
    }

    private static List<OutboxRow> rows(PreparedStatement select) throws SQLException {
        List<OutboxRow> rows = new ArrayList<>();
        try (ResultSet result = select.executeQuery()) {
            while (result.next()) {
                rows.add(new OutboxRow(result.getString(1), result.getString(2), result.getString(3),
                        result.getString(4), result.getString(5), result.getBytes(6),
                        result.getTimestamp(7).toInstant()));
            }
        }
        return rows;
    }

    private static String placeholders(int count) {
        return String.join(", ", java.util.Collections.nCopies(count, "?"));
    }
}
