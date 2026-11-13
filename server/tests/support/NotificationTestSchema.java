package support;

import java.sql.SQLException;
import java.util.List;

/**
 * Disposable Notification schema ({@code ntf_test_<uuid>}) from {@code db/notification/schema.sql}
 * (007 data-model.md section 2). {@link #reset()} returns it to the baseline: {@code subscription_clock.version}
 * 0 and no other row.
 */
public final class NotificationTestSchema extends TestSchema {
    /** Child tables first, so the foreign keys never block a reset. */
    private static final List<String> TABLES = List.of("notification", "sale_selection", "subscription",
            "subscription_change", "subscriber", "dead_letter_event");

    private NotificationTestSchema(Options options) throws Exception {
        super("ntf", "notification", options);
    }

    public static NotificationTestSchema create() throws Exception {
        return new NotificationTestSchema(Options.defaults());
    }

    public static NotificationTestSchema create(Options options) throws Exception {
        return new NotificationTestSchema(options);
    }

    /** The Notification user may only read and write rows (007 T012); the schema is applied by an administrator. */
    @Override
    protected String ownPrivileges() {
        return "SELECT, INSERT, UPDATE, DELETE";
    }

    @Override
    protected List<String> eventTables() {
        return List.of("dead_letter_event");
    }

    /** Back to the baseline: every table empty and the subscription clock at version 0. */
    public void reset() throws SQLException {
        for (String table : TABLES) update("DELETE FROM " + table);
        update("UPDATE subscription_clock SET version = 0 WHERE id = 1");
    }

    /** {@code subscription_clock.version}, the current subscription history version. */
    public long subscriptionVersion() throws SQLException {
        return queryLong("SELECT version FROM subscription_clock WHERE id = 1");
    }

    public long subscriptionChanges() throws SQLException {
        return queryLong("SELECT COUNT(*) FROM subscription_change");
    }

    public long saleSelections() throws SQLException {
        return queryLong("SELECT COUNT(*) FROM sale_selection");
    }

    public long notifications() throws SQLException {
        return queryLong("SELECT COUNT(*) FROM notification");
    }

    /** Notifications in {@code state}: {@code PENDING}, {@code DELIVERED}, or {@code PARKED}. */
    public long notifications(String state) throws SQLException {
        if (!List.of("PENDING", "DELIVERED", "PARKED").contains(state)) {
            throw new IllegalArgumentException("Unknown notification state: " + state);
        }
        return queryLong("SELECT COUNT(*) FROM notification WHERE state = ?", state);
    }
}
