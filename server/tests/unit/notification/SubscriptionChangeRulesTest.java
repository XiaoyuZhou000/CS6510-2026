package unit.notification;

import database.SubscriptionChangeRules;
import database.SubscriptionChangeRules.Apply;
import database.SubscriptionChangeRules.Decision;
import database.SubscriptionChangeRules.NoOp;
import database.SubscriptionChangeRules.Reject;
import database.SubscriptionStore.ChangeType;
import database.SubscriptionStore.Rejection;
import database.SubscriptionStore.Status;
import database.SubscriptionStore.SubscriberState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The subscription change rules (007 T081, FR-006, contracts/notification-http.md): what register, update, and
 * cancel do to a subscriber's committed state, decided under the store's locks, and the validation precedence
 * 400 → 404 → 409 → {@code UNKNOWN_SKU}.
 */
class SubscriptionChangeRulesTest {
    private static final Optional<SubscriberState> NONE = Optional.empty();

    @Test
    void addOnlyRemoveOnlyAndAddPlusRemoveGiveOneUpdateWithTheEffectiveSkusAndTheResultingSet() {
        Optional<SubscriberState> ab = active("A", "B");
        assertEquals(new Apply(ChangeType.UPDATE, List.of("C"), List.of(), List.of("A", "B", "C")),
                update(ab, set("C"), set(), null));
        assertEquals(new Apply(ChangeType.UPDATE, List.of(), List.of("A"), List.of("B")),
                update(ab, set(), set("A"), null));
        assertEquals(new Apply(ChangeType.UPDATE, List.of("C", "D"), List.of("A"), List.of("B", "C", "D")),
                update(ab, set("D", "C"), set("A"), null));
    }

    @Test
    void addingHeldSkusAndRemovingAbsentOnesIsANoOp() {
        Optional<SubscriberState> ab = active("A", "B");
        assertEquals(new NoOp(), update(ab, set("A"), set(), null));
        assertEquals(new NoOp(), update(ab, set(), set("Z"), null));
        assertEquals(new NoOp(), update(ab, set("B"), set("C"), null));
        assertEquals(new Apply(ChangeType.UPDATE, List.of(), List.of("B"), List.of("A")), update(ab, set("A"), set("B"), null),
                "only the effective part is recorded");
    }

    @Test
    void aRemovalThatWouldLeaveNoSkuIsLastSku() {
        assertEquals(Rejection.LAST_SKU, reason(update(active("A"), set(), set("A"), null)));
        assertEquals(Rejection.LAST_SKU, reason(update(active("A", "B"), set(), set("A", "B", "C"), null)));
        assertInstanceOf(Apply.class, update(active("A"), set("B"), set("A"), null), "replacing the last SKU is allowed");
    }

    @Test
    void aChangeToACancelledSubscriberIsSubscriberCancelled() {
        assertEquals(Rejection.SUBSCRIBER_CANCELLED, reason(update(cancelled(), set("A"), set(), null)));
        assertEquals(Rejection.SUBSCRIBER_CANCELLED, reason(update(cancelled(), set(), set("A"), null)));
    }

    @Test
    void cancellingRemovesEverySkuAndCancellingTwiceIsANoOp() {
        assertEquals(new Apply(ChangeType.CANCEL, List.of(), List.of("A", "B"), List.of()),
                SubscriptionChangeRules.cancel(active("A", "B")));
        assertEquals(new NoOp(), SubscriptionChangeRules.cancel(cancelled()));
        assertEquals(Rejection.SUBSCRIBER_NOT_FOUND, reason(SubscriptionChangeRules.cancel(NONE)));
    }

    @Test
    void registeringAfterACancelIsANewRegister() {
        assertEquals(new Apply(ChangeType.REGISTER, List.of("C"), List.of(), List.of("C")),
                SubscriptionChangeRules.register(cancelled(), set("C"), null));
        assertEquals(new Apply(ChangeType.REGISTER, List.of("A", "B"), List.of(), List.of("A", "B")),
                SubscriptionChangeRules.register(NONE, set("B", "A"), null));
        assertEquals(new NoOp(), SubscriptionChangeRules.register(active("A", "B"), set("A", "B"), null), "an identical repeat");
        assertEquals(Rejection.SUBSCRIBER_EXISTS, reason(SubscriptionChangeRules.register(active("A"), set("A", "B"), null)));
    }

    @Test
    void anSkuInBothListsOrTwoEmptyListsIsInvalidRequest() {
        assertEquals(Rejection.INVALID_REQUEST, reason(update(active("A"), set("B"), set("B"), null)));
        assertEquals(Rejection.INVALID_REQUEST, reason(update(active("A"), set(), set(), null)));
        assertNotNull(SubscriptionChangeRules.updateRequestProblem(set("B"), set("B")));
        assertNotNull(SubscriptionChangeRules.updateRequestProblem(set(), set()));
        assertNull(SubscriptionChangeRules.updateRequestProblem(set("A"), set()));
    }

    @Test
    void theValidationPrecedenceIs400Then404Then409ThenUnknownSku() {
        // Shape first: an invalid request is 400 whatever the subscriber's state.
        assertEquals(Rejection.INVALID_REQUEST, reason(update(NONE, set("Z"), set("Z"), "Z")));
        assertEquals(Rejection.INVALID_REQUEST, reason(update(cancelled(), set(), set(), null)));
        // Then existence.
        assertEquals(Rejection.SUBSCRIBER_NOT_FOUND, reason(update(NONE, set("Z"), set(), "Z")));
        // Then state conflicts.
        assertEquals(Rejection.SUBSCRIBER_CANCELLED, reason(update(cancelled(), set("Z"), set(), "Z")));
        assertEquals(Rejection.LAST_SKU, reason(update(active("A"), set(), set("A", "Z"), "Z")));
        assertEquals(Rejection.SUBSCRIBER_EXISTS, reason(SubscriptionChangeRules.register(active("A"), set("Z"), "Z")));
        // Then SKU validity, even for a request that would otherwise be a no-op.
        assertEquals(Rejection.UNKNOWN_SKU, reason(update(active("A"), set("Z"), set(), "Z")));
        assertEquals(Rejection.UNKNOWN_SKU, reason(update(active("A"), set(), set("Z"), "Z")));
        Reject unknown = assertInstanceOf(Reject.class, SubscriptionChangeRules.register(NONE, set("A", "Z"), "Z"));
        assertEquals(Rejection.UNKNOWN_SKU, unknown.reason());
        assertEquals("Z", unknown.detail());
    }

    private static Decision update(Optional<SubscriberState> current, SortedSet<String> add, SortedSet<String> remove,
                                   String unknown) {
        return SubscriptionChangeRules.update(current, add, remove, unknown);
    }

    private static Rejection reason(Decision decision) {
        return assertInstanceOf(Reject.class, decision, () -> "expected a rejection, got " + decision).reason();
    }

    private static Optional<SubscriberState> active(String... skus) {
        return Optional.of(new SubscriberState("s1", Status.ACTIVE, List.of(skus), "t1", true, 1, 3));
    }

    private static Optional<SubscriberState> cancelled() {
        return Optional.of(new SubscriberState("s1", Status.CANCELLED, List.of(), "t1", true, 1, 4));
    }

    private static SortedSet<String> set(String... skus) {
        return new TreeSet<>(List.of(skus));
    }
}
