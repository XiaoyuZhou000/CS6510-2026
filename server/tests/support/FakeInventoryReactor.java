package support;

import database.CheckoutAttemptStore.AttemptKey;
import events.EnvelopeCodec;
import events.EventTypes;
import events.RecordStream;
import events.TopicNames;
import selfcheckout.events.v1.AttemptId;
import selfcheckout.events.v1.CheckoutLine;
import selfcheckout.events.v1.CheckoutRequested;
import selfcheckout.events.v1.RejectionReason;
import selfcheckout.events.v1.StockDeducted;
import selfcheckout.events.v1.StockDeductionRefused;
import selfcheckout.events.v1.StockRejected;
import selfcheckout.events.v1.StockRestored;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A scriptable Inventory for Transaction's checkout tests (D14; replaces {@code FakeInventoryGateway} for
 * checkout). It consumes {@code CheckoutRequested} from an {@link InMemoryEventBus} as group
 * {@code inventory-checkout} and publishes outcome events onto the outcome topic, as Inventory would.
 *
 * <ul>
 *   <li>Every request is recorded as a {@link Call}. The first request of an attempt is answered by the next
 *       scripted {@link Reply}, or by the default once the script is used up; that answer is the attempt's
 *       recorded outcome. A repeated request re-sends the recorded outcome with the same {@code event_id},
 *       like Inventory's re-send by {@code outcome_event_id} (D8).</li>
 *   <li>Replies can be delayed, dropped in transit (recorded but not sent, so only a republish delivers it),
 *       duplicated, or {@link Reply#ignore() ignored} (nothing is recorded, as if the request never arrived).</li>
 * </ul>
 */
public final class FakeInventoryReactor implements AutoCloseable {
    /** One consumed request, with the thread that consumed it. */
    public record Call(AttemptKey key, List<Line> lines, String eventId, Thread thread) { }

    public record Line(String sku, int quantity) { }

    public enum Kind { DEDUCT, REJECT, REFUSE, RESTORED, NOT_NEEDED, IGNORE }

    /** A scripted answer; {@code delay}, {@code dropped}, and {@code copies} change only its delivery. */
    public record Reply(Kind kind, RejectionReason reason, String sku, Duration delay, boolean dropped, int copies) {
        public static Reply deduct() { return new Reply(Kind.DEDUCT, null, null, Duration.ZERO, false, 1); }

        public static Reply reject(RejectionReason reason, String sku) {
            return new Reply(Kind.REJECT, reason, sku, Duration.ZERO, false, 1);
        }

        public static Reply insufficient(String sku) { return reject(RejectionReason.INSUFFICIENT_STOCK, sku); }

        public static Reply invalid(String sku) { return reject(RejectionReason.INVALID_REQUEST, sku); }

        public static Reply refuse() { return new Reply(Kind.REFUSE, null, null, Duration.ZERO, false, 1); }

        public static Reply restored() { return new Reply(Kind.RESTORED, null, null, Duration.ZERO, false, 1); }

        public static Reply notNeeded() { return new Reply(Kind.NOT_NEEDED, null, null, Duration.ZERO, false, 1); }

        /** No outcome is recorded or sent: the next request of the attempt is answered afresh. */
        public static Reply ignore() { return new Reply(Kind.IGNORE, null, null, Duration.ZERO, false, 1); }

        public Reply delayed(Duration by) { return new Reply(kind, reason, sku, by, dropped, copies); }

        /** Recorded, but this send is lost; a repeated request re-sends it. */
        public Reply lostInTransit() { return new Reply(kind, reason, sku, delay, true, copies); }

        public Reply duplicated(int times) { return new Reply(kind, reason, sku, delay, dropped, times); }
    }

    private record Recorded(Reply reply, String eventId, byte[] envelope) { }

    private final InMemoryEventBus bus;
    private final TopicNames names;
    private final List<Call> calls = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedQueue<Function<Call, Reply>> script = new ConcurrentLinkedQueue<>();
    private final Map<AttemptKey, Recorded> recorded = new ConcurrentHashMap<>();
    private final ScheduledExecutorService delays = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "fake-inventory-delays");
        thread.setDaemon(true);
        return thread;
    });
    private volatile Function<Call, Reply> otherwise;
    private volatile boolean running;
    private Thread thread;

    private FakeInventoryReactor(InMemoryEventBus bus, TopicNames names, Function<Call, Reply> otherwise) {
        this.bus = Objects.requireNonNull(bus, "bus");
        this.names = Objects.requireNonNull(names, "names");
        this.otherwise = otherwise;
    }

    /** Deducts every attempt; call {@link #start()} to begin consuming. */
    public static FakeInventoryReactor deducting(InMemoryEventBus bus, TopicNames names) {
        return new FakeInventoryReactor(bus, names, call -> Reply.deduct());
    }

    /** Fails the test if any request arrives. */
    public static FakeInventoryReactor failing(InMemoryEventBus bus, TopicNames names) {
        return new FakeInventoryReactor(bus, names, call -> {
            throw new AssertionError("Inventory received a checkout request for " + call.key());
        });
    }

    public FakeInventoryReactor then(Reply reply) {
        script.add(call -> reply);
        return this;
    }

    public FakeInventoryReactor thenAnswer(Function<Call, Reply> answer) {
        script.add(answer);
        return this;
    }

    public FakeInventoryReactor otherwise(Function<Call, Reply> answer) {
        this.otherwise = answer;
        return this;
    }

    public List<Call> calls() {
        return List.copyOf(calls);
    }

    /** Requests consumed so far, repeats included. */
    public int requests() {
        return calls.size();
    }

    /** Requests consumed for one attempt. */
    public long requests(AttemptKey key) {
        return calls.stream().filter(call -> call.key().equals(key)).count();
    }

    public synchronized FakeInventoryReactor start() {
        if (running) return this;
        running = true;
        thread = new Thread(this::run, "fake-inventory-reactor");
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    @Override
    public void close() throws InterruptedException {
        running = false;
        Thread current = thread;
        if (current != null) current.join(5_000);
        delays.shutdownNow();
    }

    private void run() {
        String group = names.group(TopicNames.INVENTORY_CHECKOUT_GROUP);
        try (RecordStream stream = bus.stream()) {
            stream.subscribe(group, List.of(names.checkout()), new RecordStream.Listener() {
                @Override
                public void onAssigned(Collection<RecordStream.Partition> partitions) { }

                @Override
                public void onRevoked(Collection<RecordStream.Partition> partitions) { }
            });
            while (running) {
                List<RecordStream.Record> records;
                try {
                    records = stream.poll(Duration.ofMillis(20));
                } catch (RuntimeException brokerDown) {
                    sleep(20);
                    continue;
                }
                Map<RecordStream.Partition, Long> next = new java.util.HashMap<>();
                for (RecordStream.Record record : records) {
                    react(record);
                    next.put(record.position(), record.offset() + 1);
                }
                if (!next.isEmpty()) {
                    try {
                        stream.commitSync(next);
                    } catch (RuntimeException lost) {
                        // Redelivered after the bus recovers; the reactor answers repeats from its record.
                    }
                }
            }
        }
    }

    private void react(RecordStream.Record record) {
        var event = EnvelopeCodec.decode(record.value(), record.key());
        if (event.type() != EventTypes.CHECKOUT_REQUESTED) return;
        CheckoutRequested request = event.payload(CheckoutRequested.class);
        AttemptKey key = new AttemptKey(request.getAttempt().getTransactionId(), request.getAttempt().getAttemptNumber());
        List<Line> lines = new ArrayList<>();
        for (CheckoutLine line : request.getLinesList()) lines.add(new Line(line.getSku(), line.getQuantity()));
        Call call = new Call(key, List.copyOf(lines), event.eventId(), Thread.currentThread());
        calls.add(call);
        Recorded known = recorded.get(key);
        if (known != null) {
            send(key, known.envelope(), Duration.ZERO, 1);
            return;
        }
        Function<Call, Reply> answer = script.poll();
        Reply reply = (answer != null ? answer : otherwise).apply(call);
        if (reply.kind() == Kind.IGNORE) return;
        String eventId = UUID.randomUUID().toString();
        byte[] envelope = envelope(eventId, key, reply);
        recorded.put(key, new Recorded(reply, eventId, envelope));
        if (!reply.dropped()) send(key, envelope, reply.delay(), reply.copies());
    }

    /** Publishes an outcome for {@code key} directly, for example a stale or contradicting one. */
    public void publish(AttemptKey key, Reply reply) {
        send(key, envelope(UUID.randomUUID().toString(), key, reply), reply.delay(), reply.copies());
    }

    /** The decoded outcome event {@code reply} stands for, as a consumer would hand it to its handler. */
    public static events.DecodedEvent outcomeEvent(AttemptKey key, Reply reply) {
        return EnvelopeCodec.decode(envelope(UUID.randomUUID().toString(), key, reply), key.transactionId());
    }

    private void send(AttemptKey key, byte[] envelope, Duration delay, int copies) {
        Runnable sending = () -> {
            for (int i = 0; i < copies; i++) bus.send(names.checkoutOutcome(), key.transactionId(), envelope);
        };
        if (delay.isZero()) {
            sending.run();
        } else {
            delays.schedule(sending, delay.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static byte[] envelope(String eventId, AttemptKey key, Reply reply) {
        AttemptId attempt = AttemptId.newBuilder().setTransactionId(key.transactionId())
                .setAttemptNumber(key.attemptNumber()).build();
        String id = key.transactionId();
        long now = System.currentTimeMillis();
        return switch (reply.kind()) {
            case DEDUCT -> EnvelopeCodec.encode(eventId, EventTypes.STOCK_DEDUCTED, id, id, EventTypes.INVENTORY, now,
                    StockDeducted.newBuilder().setAttempt(attempt).build());
            case REJECT -> EnvelopeCodec.encode(eventId, EventTypes.STOCK_REJECTED, id, id, EventTypes.INVENTORY, now,
                    StockRejected.newBuilder().setAttempt(attempt).setReason(reply.reason())
                            .setSku(reply.sku() == null ? "" : reply.sku()).build());
            case REFUSE -> EnvelopeCodec.encode(eventId, EventTypes.STOCK_DEDUCTION_REFUSED, id, id, EventTypes.INVENTORY,
                    now, StockDeductionRefused.newBuilder().setAttempt(attempt).build());
            case RESTORED -> EnvelopeCodec.encode(eventId, EventTypes.STOCK_RESTORED, id, id, EventTypes.INVENTORY, now,
                    StockRestored.newBuilder().setAttempt(attempt).build());
            case NOT_NEEDED -> EnvelopeCodec.encode(eventId, EventTypes.RESTORATION_NOT_NEEDED, id, id, EventTypes.INVENTORY,
                    now, selfcheckout.events.v1.RestorationNotNeeded.newBuilder().setAttempt(attempt)
                            .setResult(selfcheckout.events.v1.RestorationResult.NOTHING_TO_RESTORE).build());
            case IGNORE -> throw new IllegalArgumentException("an ignored request has no outcome");
        };
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
