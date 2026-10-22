package support;

import database.RelayStore;
import transaction.ScanEventSink;

import java.util.List;

/**
 * A relay sink that is either down, answering every call as {@code UNAVAILABLE} like an Analytics outage, or up,
 * delegating to a real sink such as a {@code GrpcAnalyticsIngestClient} on an {@link InProcessAnalytics} channel.
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
        return current == null ? new Uncertain("UNAVAILABLE") : current.deliver(events);
    }
}
