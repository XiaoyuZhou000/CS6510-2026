package notification;

import database.SelectionStore;
import database.SubscriptionStore.ChangeRow;
import database.SubscriptionStore.ChangeType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The in-memory recipient index (007 research N5, data-model.md section 4): an immutable snapshot of SKU → sorted
 * subscriber IDs, tagged with the history version it reflects. It is an acceleration structure only. It is built
 * at startup from the committed subscriptions and the clock, and then changes only by committed history rows
 * applied in strict version order (Implementation Rule 4); every selection compares its version with the locked
 * clock and catches up from {@code subscription_change} first ({@link #atVersion}).
 */
public final class SubscriptionIndex {
    /** One immutable state of the index. */
    public static final class Snapshot {
        private final long version;
        private final Map<String, List<String>> bySku;
        private final Map<String, Set<String>> bySubscriber;

        private Snapshot(long version, Map<String, Set<String>> bySubscriber) {
            this.version = version;
            Map<String, Set<String>> subscribers = new TreeMap<>();
            Map<String, TreeSet<String>> skus = new HashMap<>();
            bySubscriber.forEach((subscriber, held) -> {
                if (held.isEmpty()) return;
                subscribers.put(subscriber, Set.copyOf(held));
                for (String sku : held) skus.computeIfAbsent(sku, ignored -> new TreeSet<>()).add(subscriber);
            });
            Map<String, List<String>> index = new HashMap<>();
            skus.forEach((sku, ids) -> index.put(sku, List.copyOf(ids)));
            this.bySku = Map.copyOf(index);
            this.bySubscriber = Map.copyOf(subscribers);
        }

        public long version() { return version; }

        /** The subscribers of {@code sku}, sorted; empty when nobody subscribes to it. */
        public List<String> lookup(String sku) {
            return bySku.getOrDefault(sku, List.of());
        }

        /** The SKUs each active subscriber holds. */
        public Map<String, Set<String>> subscriptions() { return bySubscriber; }

        /** SKU → sorted subscribers, for comparisons in tests and diagnostics. */
        public Map<String, List<String>> asMap() { return new TreeMap<>(bySku); }

        public int subscribers() { return bySubscriber.size(); }

        @Override
        public boolean equals(Object other) {
            return other instanceof Snapshot that && version == that.version && bySku.equals(that.bySku);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, bySku);
        }

        @Override
        public String toString() {
            return "Snapshot[version " + version + ", " + asMap() + "]";
        }
    }

    /** The result of applying one history row. */
    public enum Applied { APPLIED, ALREADY_APPLIED, GAP }

    private final AtomicReference<Snapshot> current = new AtomicReference<>(new Snapshot(0, Map.of()));

    public SubscriptionIndex() { }

    /** The current snapshot; never null. */
    public Snapshot snapshot() { return current.get(); }

    public long version() { return current.get().version(); }

    public List<String> lookup(String sku) { return current.get().lookup(sku); }

    /** Replaces the index with the committed subscriptions {@code skusBySubscriber} at history version {@code version}. */
    public synchronized void rebuild(Map<String, ? extends Collection<String>> skusBySubscriber, long version) {
        if (version < 0) throw new IllegalArgumentException("version must not be negative");
        Map<String, Set<String>> state = new HashMap<>();
        skusBySubscriber.forEach((subscriber, skus) -> state.put(subscriber, new TreeSet<>(skus)));
        current.set(new Snapshot(version, state));
    }

    /**
     * Applies one committed history row. It must be the next version: an older one is
     * {@link Applied#ALREADY_APPLIED} and changes nothing, and a later one is a {@link Applied#GAP} the caller fills
     * from the database first. The row's {@code resulting_skus} replaces the subscriber's set; a {@code CANCEL}
     * removes it.
     */
    public synchronized Applied apply(ChangeRow change) {
        Snapshot snapshot = current.get();
        if (change.version() <= snapshot.version()) return Applied.ALREADY_APPLIED;
        if (change.version() != snapshot.version() + 1) return Applied.GAP;
        Map<String, Set<String>> state = new HashMap<>(snapshot.subscriptions());
        if (change.type() == ChangeType.CANCEL || change.resultingSkus().isEmpty()) state.remove(change.subscriberId());
        else state.put(change.subscriberId(), new TreeSet<>(change.resultingSkus()));
        current.set(new Snapshot(change.version(), state));
        return Applied.APPLIED;
    }

    /**
     * Applies {@code changes} in order; rows already applied are skipped. Returns false, applying nothing further,
     * at the first gap.
     */
    public synchronized boolean apply(List<ChangeRow> changes) {
        for (ChangeRow change : changes) {
            if (apply(change) == Applied.GAP) return false;
        }
        return true;
    }

    /**
     * The snapshot at exactly {@code lockedVersion}, the clock version a selection holds {@code FOR SHARE}: when the
     * index is behind, it first applies the rows in {@code (version, lockedVersion]} read through {@code changes}.
     *
     * @throws IllegalStateException if the history has a hole or the index is ahead of the clock, which committed
     *                               history and the clock lock rule out
     */
    public Snapshot atVersion(long lockedVersion, SelectionStore.ChangeSource changes) {
        Snapshot snapshot = current.get();
        if (snapshot.version() == lockedVersion) return snapshot;
        if (snapshot.version() < lockedVersion) {
            synchronized (this) {
                snapshot = current.get();
                if (snapshot.version() < lockedVersion) {
                    List<ChangeRow> rows = new ArrayList<>(changes.changesAfter(snapshot.version(), lockedVersion));
                    if (!apply(rows)) {
                        throw new IllegalStateException("subscription history has a gap after version " + version());
                    }
                }
                snapshot = current.get();
            }
        }
        if (snapshot.version() != lockedVersion) {
            throw new IllegalStateException("subscription index is at version " + snapshot.version()
                    + " but the locked clock is at " + lockedVersion);
        }
        return snapshot;
    }
}
