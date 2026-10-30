package support;

import database.RelayStore;
import transaction.ScanEventSink;

import java.util.List;

/**
 * A relay sink that is either down, answering every batch as {@code Uncertain} like an unreachable broker, or up,
 * delegating to a real sink such as a {@code KafkaScanEventSink} on an {@link InMemoryEventBus} or live Kafka.
 */
public final class SwitchableScanEventSink implements ScanEventSink {
    private volatile ScanEventSink target;

    /** Starts down. */
    public SwitchableScanEventSink() { }

    public void up(ScanEventSink delegate) {
        target = delegate;
    }

    public void down() {
        target = null;
    }

    @Override
    public DeliveryResult deliver(List<RelayStore.UndeliveredScan> events) {
        ScanEventSink current = target;
        return current == null ? new Uncertain("broker unreachable") : current.deliver(events);
    }
}
