package unit.transaction;

import database.AcceptedScanStore;
import database.CheckoutCompletionStore;
import database.StoreFailure;
import database.TransactionStore;
import org.junit.jupiter.api.Test;
import transaction.TransactionFailure;
import transaction.TransactionOperations;
import transaction.TransactionService;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TransactionServiceTest {
    private final Rows rows=new Rows();
    private final Scans scans=new Scans();
    private final Completions completions=new Completions();
    private final TransactionService service=new TransactionService(rows,scans,completions);

    @Test
    void startScanCompleteAndDurableLookupAcrossServiceReconstruction() {
        var started=service.start(new TransactionOperations.StartCommand("station-1"));
        assertEquals(TransactionOperations.TransactionStatus.OPEN,started.status());
        var scan=service.scan(new TransactionOperations.ScanCommand(started.transactionId(),"A"));
        assertEquals(1,scan.itemCount());
        assertEquals(new BigDecimal("1.25"),scan.runningTotal());
        assertEquals(1,scans.accepted);
        var restarted=new TransactionService(rows,scans,completions);
        assertEquals(started.startedAt(),restarted.get(started.transactionId()).startedAt());
        assertEquals(1,restarted.get(started.transactionId()).itemCount());
        var receipt=restarted.complete(started.transactionId());
        assertTrue(completions.committedBeforeReturn);
        assertEquals(1,receipt.itemCount());
        assertEquals(new BigDecimal("1.25"),receipt.totalAmount());
        assertEquals(1,receipt.lines().size());
        assertEquals(TransactionOperations.TransactionStatus.COMPLETED,restarted.get(started.transactionId()).status());
    }

    @Test
    void reportsMissingUnknownEmptyInsufficientAndDuplicateCases() {
        assertFailure(TransactionFailure.Code.NOT_FOUND,() -> service.get("missing"));
        String id=service.start(new TransactionOperations.StartCommand("station-2")).transactionId();
        assertFailure(TransactionFailure.Code.NOT_FOUND,
            () -> service.scan(new TransactionOperations.ScanCommand("missing","A")));
        assertFailure(TransactionFailure.Code.UNKNOWN_SKU,
            () -> service.scan(new TransactionOperations.ScanCommand(id,"missing")));
        assertEquals(0,scans.accepted);
        assertFailure(TransactionFailure.Code.EMPTY_BASKET,() -> service.complete(id));
        service.scan(new TransactionOperations.ScanCommand(id,"A"));
        completions.insufficient=true;
        assertFailure(TransactionFailure.Code.INSUFFICIENT_STOCK,() -> service.complete(id));
        completions.insufficient=false;
        service.complete(id);
        assertFailure(TransactionFailure.Code.NOT_OPEN,() -> service.complete(id));
        assertFailure(TransactionFailure.Code.NOT_OPEN,
            () -> service.scan(new TransactionOperations.ScanCommand(id,"A")));
    }

    @Test
    void failedStoreWriteCannotAcknowledgeScanOrCorruptAnotherBasket() {
        String first=service.start(new TransactionOperations.StartCommand("one")).transactionId();
        String second=service.start(new TransactionOperations.StartCommand("two")).transactionId();
        scans.failure=new StoreFailure("database unavailable");
        assertThrows(StoreFailure.class,() -> service.scan(new TransactionOperations.ScanCommand(first,"A")));
        assertEquals(0,service.get(first).itemCount());
        scans.failure=null;
        assertEquals(1,service.scan(new TransactionOperations.ScanCommand(second,"B")).itemCount());
        assertEquals(0,service.get(first).itemCount());
    }

    private static void assertFailure(TransactionFailure.Code code,Runnable action) {
        assertEquals(code,assertThrows(TransactionFailure.class,action::run).code());
    }

    private final class Rows implements TransactionStore {
        final Map<String,TransactionRecord> records=new HashMap<>();
        final Map<String,Map<String,TransactionLine>> lines=new HashMap<>();
        public void insertOpen(String id,String station) {
            records.put(id,new TransactionRecord(id,station,TransactionStatus.OPEN,
                new BigDecimal("0.00"),Instant.now(),null,0));
            lines.put(id,new HashMap<>());
        }
        public Optional<TransactionRecord> findById(String id) { return Optional.ofNullable(records.get(id)); }
        public List<TransactionLine> findLines(String id) { return List.copyOf(lines.get(id).values()); }
    }
    private final class Scans implements AcceptedScanStore {
        int accepted;
        StoreFailure failure;
        public ScanResult scanAtomically(String id,String sku) {
            if (failure!=null) throw failure;
            var row=rows.records.get(id);
            if (row==null) return new MissingTransaction();
            if (row.status()!=TransactionStore.TransactionStatus.OPEN) return new NotOpen();
            BigDecimal price=switch(sku) {
                case "A" -> new BigDecimal("1.25");
                case "B" -> new BigDecimal("2.50");
                default -> null;
            };
            if (price==null) return new UnknownSku();
            var lines=rows.lines.get(id);
            var prior=lines.get(sku);
            lines.put(sku,new TransactionStore.TransactionLine(sku,sku.equals("A")?"Apple":"Bread",
                prior==null?1:prior.quantity()+1,price));
            var total=row.totalAmount().add(price);
            int count=row.itemCount()+1;
            rows.records.put(id,new TransactionStore.TransactionRecord(id,row.stationId(),row.status(),
                total,row.startedAt(),null,count));
            return new Accepted(++accepted,sku.equals("A")?"Apple":"Bread",price,count,total);
        }
    }
    private final class Completions implements CheckoutCompletionStore {
        boolean insufficient;
        boolean committedBeforeReturn;
        public CompletionResult completeAtomically(String id) {
            var row=rows.records.get(id);
            if (row==null) return new MissingTransaction();
            if (row.status()!=TransactionStore.TransactionStatus.OPEN) return new NotOpen();
            if (row.itemCount()==0) return new EmptyBasket();
            if (insufficient) return new InsufficientStock("A");
            Instant now=Instant.now();
            rows.records.put(id,new TransactionStore.TransactionRecord(id,row.stationId(),
                TransactionStore.TransactionStatus.COMPLETED,row.totalAmount(),row.startedAt(),now,row.itemCount()));
            committedBeforeReturn=true;
            return new Completed(now);
        }
    }
}
