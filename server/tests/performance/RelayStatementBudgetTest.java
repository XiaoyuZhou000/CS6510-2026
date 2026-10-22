package performance;

import database.JdbcRelayStore;
import database.RelayStore;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.StatementBudgets;
import support.TransactionTestSchema;
import transaction.ScanEventRelay;
import transaction.ScanEventSink;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Outbox relay ordering (strict: {@code delivered_at} only after OK, one call per round) and its statement
 * budget per round (R22; contracts/scan-event-delivery.md section 2).
 */
class RelayStatementBudgetTest {

    @Test
    void deliveredAtIsWrittenOnlyAfterAnOkReplyAndNeverForAFailedCall() throws Exception {
        try (TransactionTestSchema schema = outbox(30)) {
            List<Long> undeliveredDuringCall = Collections.synchronizedList(new ArrayList<>());
            RecordingSink sink = new RecordingSink(batch -> {
                undeliveredDuringCall.add(count(schema));
                return new ScanEventSink.Uncertain("UNAVAILABLE");
            });
            ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(schema.pool()), sink, ScanEventRelay.Settings.defaults());

            assertEquals(ScanEventRelay.Round.FAILED, relay.runOnce());
            sink.answer = batch -> new ScanEventSink.InvalidBatch("malformed");
            assertEquals(ScanEventRelay.Round.FAILED, relay.runOnce());
            assertEquals(30, schema.undeliveredCount(), "a failed or refused call marks nothing");
            assertTrue(schema.deliveredIds().isEmpty());

            sink.answer = batch -> {
                undeliveredDuringCall.add(count(schema));
                return new ScanEventSink.Delivered(batch.size(), 0);
            };
            assertEquals(ScanEventRelay.Round.DELIVERED, relay.runOnce());
            assertEquals(List.of(30L, 30L), undeliveredDuringCall, "every row is still undelivered while the call runs");
            assertEquals(0, schema.undeliveredCount(), "after OK the rows sent are marked delivered");
            assertEquals(3, sink.calls.size());
            assertEquals(sink.calls.get(0), sink.calls.get(2), "the same rows are resent after a failure");
            assertEquals(ScanEventRelay.Round.IDLE, relay.runOnce());
            assertEquals(3, sink.calls.size(), "an idle round sends nothing");
        }
    }

    @Test
    void oneRoundMakesExactlyOneDeliverCallForUpTo500Events() throws Exception {
        try (TransactionTestSchema schema = outbox(1_200)) {
            RecordingSink sink = new RecordingSink(batch -> new ScanEventSink.Delivered(batch.size(), 0));
            ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(schema.pool()), sink, ScanEventRelay.Settings.defaults());
            for (int round = 0; round < 3; round++) assertEquals(ScanEventRelay.Round.DELIVERED, relay.runOnce());
            assertEquals(ScanEventRelay.Round.IDLE, relay.runOnce());
            assertEquals(List.of(500, 500, 200), sink.calls.stream().map(List::size).toList());
            List<Long> sent = new ArrayList<>();
            for (List<Long> call : sink.calls) sent.addAll(call);
            List<Long> sorted = new ArrayList<>(sent);
            Collections.sort(sorted);
            assertEquals(sorted, sent, "rows are sent in scan_id order");
            assertEquals(1_200, sent.stream().distinct().count());
            assertEquals(0, schema.undeliveredCount());
        }
    }

    @Test
    void statementsAndCommitsPerRoundStayWithinBudget() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = outbox(TransactionTestSchema.Options.defaults().decorator(counting::wrap), 800)) {
            RecordingSink sink = new RecordingSink(batch -> new ScanEventSink.Delivered(batch.size(), 0));
            ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(schema.pool()), sink, ScanEventRelay.Settings.defaults());
            for (int round = 0; round < 2; round++) {
                try (CountingConnections.Scope scope = counting.scope()) {
                    assertEquals(ScanEventRelay.Round.DELIVERED, relay.runOnce());
                    CountingConnections.Counts counts = scope.counts();
                    assertEquals(1, counts.selects());
                    assertEquals(1, counts.updates());
                    StatementBudgets.assertWithin("relay.round.statements", counts.statements());
                    StatementBudgets.assertWithin("relay.round.commits", counts.commits());
                    assertEquals(0, counts.setAutoCommitCalls() + counts.setTransactionIsolationCalls());
                }
            }
            try (CountingConnections.Scope scope = counting.scope()) {
                assertEquals(ScanEventRelay.Round.IDLE, relay.runOnce());
                assertEquals(1, scope.counts().statements(), "an idle round is one SELECT");
                assertEquals(0, scope.counts().commits());
            }
        }
    }

    private static TransactionTestSchema outbox(int scans) throws Exception {
        return outbox(TransactionTestSchema.Options.defaults(), scans);
    }

    /** A schema with {@code scans} undelivered rows spread over transactions of 50 scans each. */
    private static TransactionTestSchema outbox(TransactionTestSchema.Options options, int scans) throws Exception {
        TransactionTestSchema schema = TransactionTestSchema.create(options);
        for (int n = 0; n < scans; n++) {
            String transactionId = "tx-" + n / 50;
            if (n % 50 == 0) schema.insertOpenTransaction(transactionId);
            schema.insertScan(transactionId, n % 50 + 1, "SKU-00000" + (n % 9 + 1));
        }
        return schema;
    }

    private static long count(TransactionTestSchema schema) {
        try {
            return schema.undeliveredCount();
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    /** Records the scan_ids of every call and answers with {@link #answer}. */
    private static final class RecordingSink implements ScanEventSink {
        final List<List<Long>> calls = Collections.synchronizedList(new ArrayList<>());
        volatile Function<List<RelayStore.UndeliveredScan>, DeliveryResult> answer;

        RecordingSink(Function<List<RelayStore.UndeliveredScan>, DeliveryResult> answer) {
            this.answer = answer;
        }

        @Override
        public DeliveryResult deliver(List<RelayStore.UndeliveredScan> events) {
            calls.add(events.stream().map(RelayStore.UndeliveredScan::scanId).toList());
            return answer.apply(events);
        }
    }
}
