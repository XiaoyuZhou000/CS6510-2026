package support;

import database.CheckoutAttemptStore.AttemptKey;
import transaction.InventoryGateway;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A scriptable {@link InventoryGateway} for unit and in-process tests. Every {@code ApplyCheckout} is
 * recorded, then answered by the next scripted answer, or by the default once the script is used up.
 * Catalog calls fail the test. {@link #checkoutOutcome} reports what the fake last answered per key.
 */
public final class FakeInventoryGateway implements InventoryGateway {
    /** One recorded call, with the thread that made it. */
    public record Call(AttemptKey key, List<CheckoutLine> lines, Duration deadline, Thread thread) { }

    @FunctionalInterface
    public interface Answer {
        ApplyResult answer(Call call) throws Exception;
    }

    private final List<Call> calls = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedQueue<Answer> script = new ConcurrentLinkedQueue<>();
    private final Map<AttemptKey, RecordedOutcome> recorded = new ConcurrentHashMap<>();
    private volatile Answer otherwise;

    private FakeInventoryGateway(Answer otherwise) {
        this.otherwise = otherwise;
    }

    /** Fails the test if anything calls Inventory. */
    public static FakeInventoryGateway failing() {
        return new FakeInventoryGateway(call -> {
            throw new AssertionError("Inventory was called for " + call.key());
        });
    }

    /** Applies every attempt. */
    public static FakeInventoryGateway applying() {
        return new FakeInventoryGateway(call -> new Applied(false));
    }

    public FakeInventoryGateway then(ApplyResult result) {
        script.add(call -> result);
        return this;
    }

    public FakeInventoryGateway thenAnswer(Answer answer) {
        script.add(answer);
        return this;
    }

    public FakeInventoryGateway otherwise(Answer answer) {
        this.otherwise = answer;
        return this;
    }

    public List<Call> calls() {
        return List.copyOf(calls);
    }

    public int applyCalls() {
        return calls.size();
    }

    @Override
    public CatalogVersion catalogVersion() {
        throw new AssertionError("Inventory's catalog was requested");
    }

    @Override
    public Catalog catalog() {
        throw new AssertionError("Inventory's catalog was requested");
    }

    @Override
    public ApplyResult applyCheckout(AttemptKey key, List<CheckoutLine> lines, Duration deadline) {
        Call call = new Call(key, List.copyOf(lines), deadline, Thread.currentThread());
        calls.add(call);
        Answer next = script.poll();
        ApplyResult result;
        try {
            result = (next != null ? next : otherwise).answer(call);
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
        switch (result) {
            case Applied applied -> recorded.put(key, new RecordedOutcome(Outcome.APPLIED, null));
            case Rejected rejected -> recorded.put(key, new RecordedOutcome(
                    Outcome.valueOf(rejected.reason().name()), rejected.sku()));
            case StockRestored restored -> recorded.put(key, new RecordedOutcome(Outcome.STOCK_RESTORED, null));
            default -> { }
        }
        return result;
    }

    @Override
    public RecordedOutcome checkoutOutcome(AttemptKey key) {
        return recorded.getOrDefault(key, new RecordedOutcome(Outcome.NOT_PROCESSED, null));
    }
}
