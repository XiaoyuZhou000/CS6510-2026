package database;

import faults.FaultHooks;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * {@link SubscriptionStore} on Notification's pool (007 T049, T087). Register, update, and cancel are the same
 * contended unit of work:
 *
 * <ol>
 *   <li>{@code SELECT version FROM subscription_clock WHERE id = 1 FOR UPDATE} gives {@code V};</li>
 *   <li>{@code SELECT … FROM subscriber WHERE subscriber_id = ? FOR UPDATE}, and its {@code subscription} rows;</li>
 *   <li>{@link SubscriptionChangeRules} decides: reject or no-op (rollback), or apply. Applying writes
 *       {@code subscriber} at {@code V + 1} (insert, reactivate, {@code updated_version}, or {@code status =
 *       'CANCELLED'}), inserts and deletes {@code subscription} rows (one statement each), inserts the
 *       {@code subscription_change} row at {@code V + 1}, and sets the clock to {@code V + 1};</li>
 *   <li>{@code FaultHooks.hit("notification.subscription.beforeCommit")}, then commit.</li>
 * </ol>
 *
 * <p>At most eight statements and one commit for an applied change (an update that both adds and removes), within the
 * plan's ≤ 9.</p>
 */
public final class JdbcSubscriptionStore implements SubscriptionStore {
    private static final String LOCK_CLOCK = "SELECT version FROM subscription_clock WHERE id = 1 FOR UPDATE";
    private static final String SHARE_CLOCK = "SELECT version FROM subscription_clock WHERE id = 1 FOR SHARE";
    private static final String READ_CLOCK = "SELECT version FROM subscription_clock WHERE id = 1";
    private static final String LOCK_SUBSCRIBER = "SELECT status, channel_topic, channel_ready, created_version, "
            + "updated_version FROM subscriber WHERE subscriber_id = ? FOR UPDATE";
    private static final String SUBSCRIPTIONS = "SELECT sku FROM subscription WHERE subscriber_id = ? ORDER BY sku";
    private static final String INSERT_SUBSCRIBER = "INSERT INTO subscriber (subscriber_id, status, channel_topic, "
            + "channel_ready, created_version, updated_version, created_at, updated_at) VALUES (?, 'ACTIVE', ?, FALSE, ?, ?, ?, ?)";
    private static final String REACTIVATE = "UPDATE subscriber SET status = 'ACTIVE', updated_version = ?, updated_at = ? "
            + "WHERE subscriber_id = ?";
    private static final String TOUCH = "UPDATE subscriber SET updated_version = ?, updated_at = ? WHERE subscriber_id = ?";
    private static final String CANCEL_SUBSCRIBER = "UPDATE subscriber SET status = 'CANCELLED', updated_version = ?, "
            + "updated_at = ? WHERE subscriber_id = ?";
    private static final String DELETE_SOME = "DELETE FROM subscription WHERE subscriber_id = ? AND sku IN (";
    private static final String DELETE_ALL = "DELETE FROM subscription WHERE subscriber_id = ?";
    private static final String INSERT_CHANGE = "INSERT INTO subscription_change (version, subscriber_id, change_type, "
            + "added_skus, removed_skus, resulting_skus, committed_at) VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String ADVANCE_CLOCK = "UPDATE subscription_clock SET version = ? WHERE id = 1";
    private static final String FIND = "SELECT s.status, s.channel_topic, s.channel_ready, s.created_version, "
            + "s.updated_version, sub.sku FROM subscriber s LEFT JOIN subscription sub ON sub.subscriber_id = s.subscriber_id "
            + "WHERE s.subscriber_id = ? ORDER BY sub.sku";
    private static final String ACTIVE_SUBSCRIPTIONS = "SELECT subscriber_id, sku FROM subscription ORDER BY subscriber_id, sku";
    private static final String CHANGES = "SELECT version, subscriber_id, change_type, added_skus, removed_skus, "
            + "resulting_skus FROM subscription_change WHERE version > ? AND version <= ? ORDER BY version";
    private static final String CHANNEL = "SELECT channel_topic, channel_ready FROM subscriber WHERE subscriber_id = ?";
    private static final String MARK_READY = "UPDATE subscriber SET channel_ready = TRUE WHERE subscriber_id = ?";

    private final ConnectionPool pool;

    public JdbcSubscriptionStore(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    @Override
    public ChangeResult register(String subscriberId, SortedSet<String> skus, String channelTopic,
                                 String firstUnknownSku, Instant at) {
        Objects.requireNonNull(channelTopic, "channelTopic");
        if (skus == null || skus.isEmpty()) throw new IllegalArgumentException("a registration needs at least one SKU");
        SortedSet<String> requested = new TreeSet<>(skus);
        return change(subscriberId, channelTopic, existing -> SubscriptionChangeRules.register(existing, requested,
                firstUnknownSku), at);
    }

    @Override
    public ChangeResult update(String subscriberId, SortedSet<String> add, SortedSet<String> remove,
                               String firstUnknownSku, Instant at) {
        SortedSet<String> adding = add == null ? new TreeSet<>() : new TreeSet<>(add);
        SortedSet<String> removing = remove == null ? new TreeSet<>() : new TreeSet<>(remove);
        return change(subscriberId, null, existing -> SubscriptionChangeRules.update(existing, adding, removing,
                firstUnknownSku), at);
    }

    @Override
    public ChangeResult cancel(String subscriberId, Instant at) {
        return change(subscriberId, null, SubscriptionChangeRules::cancel, at);
    }

    /**
     * The change transaction shared by register, update, and cancel: lock the clock, lock and read the subscriber,
     * let {@code rule} decide, then write the subscriber, its subscription rows, one history row, and the clock, and
     * hit {@code notification.subscription.beforeCommit} before the commit. {@code channelTopic} is used only for a
     * subscriber's first registration.
     */
    private ChangeResult change(String subscriberId, String channelTopic,
                                Function<Optional<SubscriberState>, SubscriptionChangeRules.Decision> rule, Instant at) {
        Objects.requireNonNull(subscriberId, "subscriberId");
        Timestamp now = Timestamp.from(at.truncatedTo(ChronoUnit.MILLIS));
        return JdbcUnitOfWork.inContendedTransaction(pool, "notification.subscription.change", connection -> {
            long version = clock(connection, LOCK_CLOCK);
            Optional<SubscriberState> existing = lockSubscriber(connection, subscriberId);
            SubscriptionChangeRules.Decision decision = rule.apply(existing);
            if (decision instanceof SubscriptionChangeRules.Reject reject) {
                throw JdbcUnitOfWork.rollbackAndReturn(new Rejected(reject.reason(), reject.detail()));
            }
            if (decision instanceof SubscriptionChangeRules.NoOp) {
                throw JdbcUnitOfWork.rollbackAndReturn(new Unchanged(existing.orElseThrow(
                        () -> new IllegalStateException("a no-op needs an existing subscriber"))));
            }
            SubscriptionChangeRules.Apply apply = (SubscriptionChangeRules.Apply) decision;
            long next = version + 1;
            SubscriberState state = switch (apply.type()) {
                case REGISTER -> writeRegistration(connection, subscriberId, existing, channelTopic, apply, next, now);
                case UPDATE -> writeUpdate(connection, existing.orElseThrow(), apply, next, now);
                case CANCEL -> writeCancel(connection, existing.orElseThrow(), next, now);
            };
            ChangeRow change = new ChangeRow(next, subscriberId, apply.type(), apply.added(), apply.removed(), apply.resulting());
            insertChange(connection, change, now);
            try (PreparedStatement advance = connection.prepareStatement(ADVANCE_CLOCK)) {
                advance.setLong(1, next);
                advance.executeUpdate();
            }
            FaultHooks.hit("notification.subscription.beforeCommit");
            return new Applied(state, change, apply.type() == ChangeType.REGISTER);
        });
    }

    private static SubscriberState writeRegistration(Connection connection, String subscriberId,
                                                     Optional<SubscriberState> existing, String channelTopic,
                                                     SubscriptionChangeRules.Apply apply, long next, Timestamp now)
            throws SQLException {
        SubscriberState state;
        if (existing.isEmpty()) {
            Objects.requireNonNull(channelTopic, "channelTopic");
            try (PreparedStatement insert = connection.prepareStatement(INSERT_SUBSCRIBER)) {
                insert.setString(1, subscriberId);
                insert.setString(2, channelTopic);
                insert.setLong(3, next);
                insert.setLong(4, next);
                insert.setTimestamp(5, now);
                insert.setTimestamp(6, now);
                insert.executeUpdate();
            }
            state = new SubscriberState(subscriberId, Status.ACTIVE, apply.resulting(), channelTopic, false, next, next);
        } else {
            try (PreparedStatement update = connection.prepareStatement(REACTIVATE)) {
                update.setLong(1, next);
                update.setTimestamp(2, now);
                update.setString(3, subscriberId);
                update.executeUpdate();
            }
            SubscriberState old = existing.get();
            state = new SubscriberState(subscriberId, Status.ACTIVE, apply.resulting(), old.channelTopic(), old.channelReady(),
                    old.createdVersion(), next);
        }
        insertSubscriptions(connection, subscriberId, apply.resulting(), next);
        return state;
    }

    private static SubscriberState writeUpdate(Connection connection, SubscriberState old, SubscriptionChangeRules.Apply apply,
                                               long next, Timestamp now) throws SQLException {
        touch(connection, TOUCH, old.subscriberId(), next, now);
        if (!apply.added().isEmpty()) insertSubscriptions(connection, old.subscriberId(), apply.added(), next);
        if (!apply.removed().isEmpty()) {
            StringBuilder sql = new StringBuilder(DELETE_SOME);
            for (int i = 0; i < apply.removed().size(); i++) sql.append(i == 0 ? "?" : ", ?");
            try (PreparedStatement delete = connection.prepareStatement(sql.append(")").toString())) {
                delete.setString(1, old.subscriberId());
                for (int i = 0; i < apply.removed().size(); i++) delete.setString(i + 2, apply.removed().get(i));
                delete.executeUpdate();
            }
        }
        return new SubscriberState(old.subscriberId(), Status.ACTIVE, apply.resulting(), old.channelTopic(), old.channelReady(),
                old.createdVersion(), next);
    }

    private static SubscriberState writeCancel(Connection connection, SubscriberState old, long next, Timestamp now)
            throws SQLException {
        touch(connection, CANCEL_SUBSCRIBER, old.subscriberId(), next, now);
        try (PreparedStatement delete = connection.prepareStatement(DELETE_ALL)) {
            delete.setString(1, old.subscriberId());
            delete.executeUpdate();
        }
        return new SubscriberState(old.subscriberId(), Status.CANCELLED, List.of(), old.channelTopic(), old.channelReady(),
                old.createdVersion(), next);
    }

    private static void touch(Connection connection, String sql, String subscriberId, long version, Timestamp now)
            throws SQLException {
        try (PreparedStatement update = connection.prepareStatement(sql)) {
            update.setLong(1, version);
            update.setTimestamp(2, now);
            update.setString(3, subscriberId);
            update.executeUpdate();
        }
    }

    @Override
    public Optional<SubscriberState> find(String subscriberId) {
        return JdbcUnitOfWork.read(pool, "notification.subscription.find", connection -> {
            try (PreparedStatement select = connection.prepareStatement(FIND)) {
                select.setString(1, subscriberId);
                try (ResultSet rows = select.executeQuery()) {
                    if (!rows.next()) return Optional.empty();
                    Status status = Status.valueOf(rows.getString(1));
                    String topic = rows.getString(2);
                    boolean ready = rows.getBoolean(3);
                    long created = rows.getLong(4);
                    long updated = rows.getLong(5);
                    List<String> skus = new ArrayList<>();
                    do {
                        String sku = rows.getString(6);
                        if (sku != null) skus.add(sku);
                    } while (rows.next());
                    return Optional.of(new SubscriberState(subscriberId, status, skus, topic, ready, created, updated));
                }
            }
        });
    }

    @Override
    public IndexRows loadIndex() {
        return JdbcUnitOfWork.read(pool, "notification.subscription.loadIndex", connection -> {
            // The shared lock keeps every change from committing until the subscriptions are read, so the rows
            // are exactly the state at the clock's version under READ COMMITTED too (N5).
            long version = clock(connection, SHARE_CLOCK);
            Map<String, List<String>> bySubscriber = new TreeMap<>();
            try (PreparedStatement select = connection.prepareStatement(ACTIVE_SUBSCRIPTIONS);
                 ResultSet rows = select.executeQuery()) {
                while (rows.next()) {
                    bySubscriber.computeIfAbsent(rows.getString(1), ignored -> new ArrayList<>()).add(rows.getString(2));
                }
            }
            return new IndexRows(version, bySubscriber);
        });
    }

    @Override
    public List<ChangeRow> changesAfter(long after, long upTo) {
        return JdbcUnitOfWork.read(pool, "notification.subscription.changes", connection -> readChanges(connection, after, upTo));
    }

    @Override
    public long currentVersion() {
        return JdbcUnitOfWork.read(pool, "notification.subscription.clock", connection -> clock(connection, READ_CLOCK));
    }

    @Override
    public Optional<Channel> channel(String subscriberId) {
        return JdbcUnitOfWork.read(pool, "notification.subscription.channel", connection -> {
            try (PreparedStatement select = connection.prepareStatement(CHANNEL)) {
                select.setString(1, subscriberId);
                try (ResultSet row = select.executeQuery()) {
                    if (!row.next()) return Optional.empty();
                    return Optional.of(new Channel(subscriberId, row.getString(1), row.getBoolean(2)));
                }
            }
        });
    }

    @Override
    public void markChannelReady(String subscriberId) {
        JdbcUnitOfWork.inTransaction(pool, "notification.subscription.channelReady", connection -> {
            try (PreparedStatement update = connection.prepareStatement(MARK_READY)) {
                update.setString(1, subscriberId);
                return update.executeUpdate();
            }
        });
    }

    /** The history rows in {@code (after, upTo]} on {@code connection}, in version order; also used by selections. */
    public static List<ChangeRow> readChanges(Connection connection, long after, long upTo) throws SQLException {
        List<ChangeRow> changes = new ArrayList<>();
        if (upTo <= after) return changes;
        try (PreparedStatement select = connection.prepareStatement(CHANGES)) {
            select.setLong(1, after);
            select.setLong(2, upTo);
            try (ResultSet rows = select.executeQuery()) {
                while (rows.next()) {
                    changes.add(new ChangeRow(rows.getLong(1), rows.getString(2), ChangeType.valueOf(rows.getString(3)),
                            NotificationJson.parseSkuArray(rows.getString(4)), NotificationJson.parseSkuArray(rows.getString(5)),
                            NotificationJson.parseSkuArray(rows.getString(6))));
                }
            }
        }
        return changes;
    }

    private static long clock(Connection connection, String sql) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(sql); ResultSet row = select.executeQuery()) {
            if (!row.next()) throw new SQLException("subscription_clock has no row");
            return row.getLong(1);
        }
    }

    private static Optional<SubscriberState> lockSubscriber(Connection connection, String subscriberId) throws SQLException {
        Status status;
        String topic;
        boolean ready;
        long created;
        long updated;
        try (PreparedStatement select = connection.prepareStatement(LOCK_SUBSCRIBER)) {
            select.setString(1, subscriberId);
            try (ResultSet row = select.executeQuery()) {
                if (!row.next()) return Optional.empty();
                status = Status.valueOf(row.getString(1));
                topic = row.getString(2);
                ready = row.getBoolean(3);
                created = row.getLong(4);
                updated = row.getLong(5);
            }
        }
        List<String> skus = new ArrayList<>();
        try (PreparedStatement select = connection.prepareStatement(SUBSCRIPTIONS)) {
            select.setString(1, subscriberId);
            try (ResultSet rows = select.executeQuery()) {
                while (rows.next()) skus.add(rows.getString(1));
            }
        }
        return Optional.of(new SubscriberState(subscriberId, status, skus, topic, ready, created, updated));
    }

    private static void insertSubscriptions(Connection connection, String subscriberId, List<String> skus, long version)
            throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO subscription (subscriber_id, sku, added_version) VALUES ");
        for (int i = 0; i < skus.size(); i++) sql.append(i == 0 ? "(?, ?, ?)" : ", (?, ?, ?)");
        try (PreparedStatement insert = connection.prepareStatement(sql.toString())) {
            int parameter = 1;
            for (String sku : skus) {
                insert.setString(parameter++, subscriberId);
                insert.setString(parameter++, sku);
                insert.setLong(parameter++, version);
            }
            insert.executeUpdate();
        }
    }

    private static void insertChange(Connection connection, ChangeRow change, Timestamp at) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement(INSERT_CHANGE)) {
            insert.setLong(1, change.version());
            insert.setString(2, change.subscriberId());
            insert.setString(3, change.type().name());
            insert.setString(4, NotificationJson.skuArray(change.addedSkus()));
            insert.setString(5, NotificationJson.skuArray(change.removedSkus()));
            insert.setString(6, NotificationJson.skuArray(change.resultingSkus()));
            insert.setTimestamp(7, at);
            insert.executeUpdate();
        }
    }
}
