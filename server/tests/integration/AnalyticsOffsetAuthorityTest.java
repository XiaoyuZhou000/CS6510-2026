package integration;

import events.TopicNames;
import org.junit.jupiter.api.Test;
import support.AnalyticsTestSchema;
import support.InMemoryEventBus;
import support.InProcessAnalytics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Analytics' database, not the broker, is the authority for how far the scan topic was consumed (FR-049, D10;
 * contracts/scan-event-delivery.md section 3). A restarted Analytics resumes from {@code consumer_offset} whatever
 * the group's committed offset says; a stored offset beyond the log end, as after a recreated topic, raises an
 * alarm and reads from the earliest offset, and deduplication absorbs the overlap.
 */
class AnalyticsOffsetAuthorityTest {
    private static final Duration CONSUMED = Duration.ofSeconds(30);

    @Test
    void lostOrResetBrokerOffsetsResumeFromConsumerOffsetWithoutLossOrDoubleCount() throws Exception {
        TopicNames names = TopicNames.unprefixed();
        String group = names.group(TopicNames.ANALYTICS_SCAN_INGEST_GROUP);
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 49);
            try (InProcessAnalytics first = InProcessAnalytics.over(schema.pool(), bus, names)) {
                assertEquals(1_200, first.deliver(InProcessAnalytics.events("tx-a", 1, skus(1_200))).stored());
            }
            assertEquals(1_200L, bus.committedOffset(group, names.scanAccepted(), 0));
            bus.resetCommittedOffsets(group);
            assertNull(bus.committedOffset(group, names.scanAccepted(), 0), "the group's offsets are gone");

            try (InProcessAnalytics restarted = InProcessAnalytics.over(schema.pool(), bus, names)) {
                InProcessAnalytics.Delivery more = restarted.deliver(InProcessAnalytics.events("tx-b", 1, skus(300)));
                assertEquals(300, more.stored());
                assertEquals(0, more.duplicates(), "nothing before the stored offset is read again");
                assertEquals(0, restarted.duplicates());
                assertEquals(0, restarted.consumer.offsetResets());
            }
            assertEquals(1_500, schema.lastPosition(), "no scan is lost or counted twice");
            schema.assertPositionsContiguous();
            assertEquals(OptionalLong.of(1_500), new database.JdbcScanIngestStore(schema.pool(), new analytics.ProductNameMap())
                    .readNextOffset(names.scanAccepted(), 0));
        }
    }

    @Test
    void storedOffsetBeyondTheLogEndAlarmsReadsFromTheEarliestAndDeduplicatesTheOverlap() throws Exception {
        TopicNames names = TopicNames.unprefixed();
        try (AnalyticsTestSchema schema = AnalyticsTestSchema.create()) {
            InMemoryEventBus bus = InMemoryEventBus.withCatalogTopics(names, 50);
            List<String> skus = skus(600);
            try (InProcessAnalytics first = InProcessAnalytics.over(schema.pool(), bus, names)) {
                assertEquals(600, first.deliver(InProcessAnalytics.events("tx-a", 1, skus)).stored());
            }
            // The topic is recreated without an Analytics reset; the relay re-sends 100 old scans and 100 new ones.
            bus.recreateTopic(names.scanAccepted());
            var overlap = new ArrayList<>(InProcessAnalytics.events("tx-a", 501, skus.subList(500, 600)));
            overlap.addAll(InProcessAnalytics.events("tx-b", 1, skus(100)));
            try (InProcessAnalytics restarted = InProcessAnalytics.over(schema.pool(), bus, names)) {
                restarted.publish(overlap);
                long deadline = System.nanoTime() + CONSUMED.toNanos();
                while (restarted.stored() + restarted.duplicates() < 200) {
                    if (System.nanoTime() > deadline) fail("the recreated topic was not read from its earliest offset");
                    Thread.sleep(10);
                }
                assertEquals(1, restarted.consumer.offsetResets(), "the stored offset 600 lies beyond the end 200");
                assertEquals(100, restarted.stored(), "only the new scans are stored");
                assertEquals(100, restarted.duplicates(), "the overlap is absorbed by deduplication");
            }
            assertEquals(700, schema.lastPosition());
            schema.assertPositionsContiguous();
            assertEquals(OptionalLong.of(200), new database.JdbcScanIngestStore(schema.pool(), new analytics.ProductNameMap())
                    .readNextOffset(names.scanAccepted(), 0), "the stored offset follows the new log");
        }
    }

    private static List<String> skus(int count) {
        List<String> skus = new ArrayList<>(count);
        for (int i = 0; i < count; i++) skus.add("sku" + (1 + i % 12));
        return skus;
    }
}
