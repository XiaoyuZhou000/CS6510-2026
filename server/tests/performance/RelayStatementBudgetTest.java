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
 * Scan relay ordering (strict: {@code published_at} only after every broker confirmation, one batch send per
 * round) and its final shape per round of up to 500 rows (R22, D10; contracts/scan-event-delivery.md section 2):
 * a SELECT on a connection released before sending, a batch send holding no connection, then one UPDATE of
 * {@code published_at} and a commit in a new short transaction.
 */
class RelayStatementBudgetTest {

    @Test
    void deliveredAtIsWrittenOnlyAfterAnOkReplyAndNeverForAFailedCall() throws Exception {
        try (TransactionTestSchema schema = outbox(30)) {
            List<Long> undeliveredDuringCall = Collections.synchronizedList(new ArrayList<>());
            RecordingSink sink = new RecordingSink(batch -> {
                undeliveredDuringCall.add(count(schema));
                return new ScanEventSink.Uncertain("broker unreachable");
            });
            ScanEventRelay relay = new ScanEventRelay(new JdbcRelayStore(schema.pool()), sink, ScanEventRelay.Settings.defaults());

            assertEquals(ScanEventRelay.Round.FAILED, relay.runOnce());
            sink.answer = batch -> new ScanEventSink.InvalidBatch("cannot be encoded");
            assertEquals(ScanEventRelay.Round.FAILED, relay.runOnce());
            assertEquals(30, schema.undeliveredCount(), "an unconfirmed or unencodable batch marks nothing");
            assertTrue(schema.deliveredIds().isEmpty());

            sink.answer = batch -> {
                undeliveredDuringCall.add(count(schema));
                return new ScanEventSink.Delivered(batch.size());
            };
            assertEquals(ScanEventRelay.Round.DELIVERED, relay.runOnce());
            assertEquals(List.of(30L, 30L), undeliveredDuringCall, "every row is still undelivered while the call runs");
            assertEquals(0, schema.undeliveredCount(), "after every confirmation the rows sent are marked published");
            assertEquals(3, sink.calls.size());
            assertEquals(sink.calls.get(0), sink.calls.get(2), "the same rows are resent after a failure");
            assertEquals(ScanEventRelay.Round.IDLE, relay.runOnce());
            assertEquals(3, sink.calls.size(), "an idle round sends nothing");
        }
    }

    @Test
    void oneRoundMakesExactlyOneDeliverCallForUpTo500Events() throws Exception {
        try (TransactionTestSchema schema = outbox(1_200)) {
            List<Integer> activeDuringSend = Collections.synchronizedList(new ArrayList<>());
            var pool = schema.pool();
            RecordingSink sink = new RecordingSink(batch -> {
                activeDuringSend.add(pool.occupancy().active());
                return new ScanEventSink.Delivered(batch.size());
            });
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
            assertEquals(List.of(0, 0, 0), activeDuringSend, "the batch send holds no pool connection");
            assertEquals(0, schema.undeliveredCount());
        }
    }

    @Test
    void statementsAndCommitsPerRoundStayWithinBudget() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (TransactionTestSchema schema = outbox(TransactionTestSchema.Options.defaults().decorator(counting::wrap), 800)) {
            RecordingSink sink = new RecordingSink(batch -> new ScanEventSink.Delivered(batch.size()));
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
