package database;

import database.SubscriptionStore.ChangeType;
import database.SubscriptionStore.Rejection;
import database.SubscriptionStore.Status;
import database.SubscriptionStore.SubscriberState;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * What a register, update, or cancel request does to a subscriber's committed state (007 FR-006,
 * contracts/notification-http.md). The functions are pure: {@link JdbcSubscriptionStore} calls them under the clock and
 * subscriber locks with the state it just read, and applies the {@link Decision}.
 *
 * <p>Validation precedence: request shape (400 {@code INVALID_REQUEST}), then subscriber existence (404), then state
 * conflicts (409: {@code SUBSCRIBER_EXISTS}, {@code SUBSCRIBER_CANCELLED}, {@code LAST_SKU}), then SKU validity
 * (400 {@code UNKNOWN_SKU}). Only then is a request a no-op or an applied change. {@code firstUnknownSku}, when not
 * null, is the first requested SKU outside the catalog; the caller checks it against {@code KnownSkus}.</p>
 */
public final class SubscriptionChangeRules {
    /** What to do under the locks. */
    public sealed interface Decision permits Reject, NoOp, Apply { }

    /** Rejected; nothing is written. */
    public record Reject(Rejection reason, String detail) implements Decision { }

    /** An idempotent repeat or a change with no effect; nothing is written, and the current state is the answer. */
    public record NoOp() implements Decision { }

    /** One history row: {@code added} and {@code removed} are the effective SKUs, {@code resulting} the new set. */
    public record Apply(ChangeType type, List<String> added, List<String> removed, List<String> resulting)
            implements Decision {
        public Apply {
            added = List.copyOf(new TreeSet<>(added));
            removed = List.copyOf(new TreeSet<>(removed));
            resulting = List.copyOf(new TreeSet<>(resulting));
        }
    }

    private SubscriptionChangeRules() { }

    /**
     * The shape problem of an update request, or null: at least one of the two lists must be non-empty, and an SKU may
     * not be in both.
     */
    public static String updateRequestProblem(Collection<String> add, Collection<String> remove) {
        boolean noAdd = add == null || add.isEmpty();
        boolean noRemove = remove == null || remove.isEmpty();
        if (noAdd && noRemove) return "add or remove must list at least one SKU";
        if (!noAdd && !noRemove) {
            for (String sku : add) {
                if (remove.contains(sku)) return "SKU " + sku + " is in both add and remove";
            }
        }
        return null;
    }

    /** A registration: new, or again after a cancellation. An active subscriber with the same set is a repeat. */
    public static Decision register(Optional<SubscriberState> current, SortedSet<String> skus, String firstUnknownSku) {
        List<String> requested = List.copyOf(new TreeSet<>(skus));
        if (current.isPresent() && current.get().status() == Status.ACTIVE) {
            if (current.get().skus().equals(requested)) return new NoOp();
            return new Reject(Rejection.SUBSCRIBER_EXISTS,
                    "subscriber " + current.get().subscriberId() + " is already registered with other SKUs");
        }
        if (firstUnknownSku != null) return new Reject(Rejection.UNKNOWN_SKU, firstUnknownSku);
        return new Apply(ChangeType.REGISTER, requested, List.of(), requested);
    }

    /** Adds {@code add} and removes {@code remove}; the result must keep at least one SKU. */
    public static Decision update(Optional<SubscriberState> current, SortedSet<String> add, SortedSet<String> remove,
                                  String firstUnknownSku) {
        String problem = updateRequestProblem(add, remove);
        if (problem != null) return new Reject(Rejection.INVALID_REQUEST, problem);
        if (add == null) add = new TreeSet<>();
        if (remove == null) remove = new TreeSet<>();
        if (current.isEmpty()) return new Reject(Rejection.SUBSCRIBER_NOT_FOUND, "subscriber is not registered");
        SubscriberState state = current.get();
        if (state.status() == Status.CANCELLED) {
            return new Reject(Rejection.SUBSCRIBER_CANCELLED, "subscriber " + state.subscriberId() + " is cancelled");
        }
        TreeSet<String> resulting = new TreeSet<>(state.skus());
        resulting.addAll(add);
        resulting.removeAll(remove);
        if (resulting.isEmpty()) {
            return new Reject(Rejection.LAST_SKU, "subscriber " + state.subscriberId() + " must keep at least one SKU");
        }
        if (firstUnknownSku != null) return new Reject(Rejection.UNKNOWN_SKU, firstUnknownSku);
        TreeSet<String> added = new TreeSet<>(add);
        added.removeAll(state.skus());
        TreeSet<String> removed = new TreeSet<>(remove);
        removed.retainAll(state.skus());
        if (added.isEmpty() && removed.isEmpty()) return new NoOp();
        return new Apply(ChangeType.UPDATE, List.copyOf(added), List.copyOf(removed), List.copyOf(resulting));
    }

    /** Cancels every subscription; cancelling a cancelled subscriber is a repeat. */
    public static Decision cancel(Optional<SubscriberState> current) {
        if (current.isEmpty()) return new Reject(Rejection.SUBSCRIBER_NOT_FOUND, "subscriber is not registered");
        SubscriberState state = current.get();
        if (state.status() == Status.CANCELLED) return new NoOp();
        return new Apply(ChangeType.CANCEL, List.of(), state.skus(), List.of());
    }
}
