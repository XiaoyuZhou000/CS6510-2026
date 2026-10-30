package support;

import events.RecordSender;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * A {@link RecordSender} that counts the sends made on each thread and delegates them, so a test can show that
 * a request thread never sends a record (FR-015) while a background sender does.
 */
public final class CountingRecordSender implements RecordSender {
    private final RecordSender delegate;
    private final Map<Thread, LongAdder> byThread = new ConcurrentHashMap<>();
    private final LongAdder total = new LongAdder();

    public CountingRecordSender(RecordSender delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableFuture<Void> send(String topic, String key, byte[] value) {
        byThread.computeIfAbsent(Thread.currentThread(), ignored -> new LongAdder()).increment();
        total.increment();
        return delegate.send(topic, key, value);
    }

    /** Sends made on {@code thread}. */
    public long sendsOn(Thread thread) {
        LongAdder sends = byThread.get(thread);
        return sends == null ? 0 : sends.sum();
    }

    public long sends() {
        return total.sum();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
