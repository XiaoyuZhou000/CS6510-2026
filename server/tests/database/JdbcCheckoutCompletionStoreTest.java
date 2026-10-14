package database;

import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.CheckoutDatabase;
import transaction.TransactionOperations;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class JdbcCheckoutCompletionStoreTest {
    @Test
    void commitsPersistedDistinctLinesOnceAndRejectsDuplicate() throws Exception {
        try (CheckoutDatabase db=open(5)) {
            String id=db.checkout.start(new TransactionOperations.StartCommand("station")).transactionId();
            scan(db,id,CheckoutDatabase.SKU);
            scan(db,id,CheckoutDatabase.SKU);
            scan(db,id,CheckoutDatabase.OTHER_SKU);
            var store=new JdbcCheckoutCompletionStore(db.pool);
            assertInstanceOf(CheckoutCompletionStore.Completed.class,store.completeAtomically(id));
            assertEquals(3,db.checkout.get(id).itemCount());
            assertEquals(3,db.stock());
            assertEquals(4,db.stock(CheckoutDatabase.OTHER_SKU));
            assertInstanceOf(CheckoutCompletionStore.NotOpen.class,store.completeAtomically(id));
            assertEquals(3,db.stock());
        }
    }
    @Test
    void insufficientStockRollsBackStatusAndEveryDecrement() throws Exception {
        try (CheckoutDatabase db=open(1)) {
            String id=db.checkout.start(new TransactionOperations.StartCommand("station")).transactionId();
            scan(db,id,CheckoutDatabase.OTHER_SKU);
            scan(db,id,CheckoutDatabase.SKU);
            scan(db,id,CheckoutDatabase.SKU);
            var result=new JdbcCheckoutCompletionStore(db.pool).completeAtomically(id);
            assertEquals(CheckoutDatabase.SKU,
                assertInstanceOf(CheckoutCompletionStore.InsufficientStock.class,result).sku());
            assertEquals(TransactionOperations.TransactionStatus.OPEN,db.checkout.get(id).status());
            assertEquals(1,db.stock());
            assertEquals(1,db.stock(CheckoutDatabase.OTHER_SKU));
            assertEquals(3,db.checkout.get(id).itemCount());
        }
    }
    @Test
    void contendingCompletionsCannotBothSellLastUnit() throws Exception {
        try (CheckoutDatabase db=open(1);ExecutorService workers=Executors.newFixedThreadPool(2)) {
            String first=db.checkout.start(new TransactionOperations.StartCommand("a")).transactionId();
            String second=db.checkout.start(new TransactionOperations.StartCommand("b")).transactionId();
            scan(db,first,CheckoutDatabase.SKU);
            scan(db,second,CheckoutDatabase.SKU);
            var store=new JdbcCheckoutCompletionStore(db.pool);
            List<Future<CheckoutCompletionStore.CompletionResult>> results=workers.invokeAll(List.of(
                () -> store.completeAtomically(first),() -> store.completeAtomically(second)));
            long success=0,rejected=0;
            for (var result:results) {
                var value=result.get();
                if (value instanceof CheckoutCompletionStore.Completed) success++;
                if (value instanceof CheckoutCompletionStore.InsufficientStock) rejected++;
            }
            assertEquals(1,success);
            assertEquals(1,rejected);
            assertEquals(0,db.stock());
        }
    }
    private static void scan(CheckoutDatabase db,String id,String sku) {
        db.checkout.scan(new TransactionOperations.ScanCommand(id,sku));
    }
    private static CheckoutDatabase open(int stock) throws Exception {
        try { return new CheckoutDatabase(stock); }
        catch (java.sql.SQLException unavailable) { throw new TestAbortedException("MySQL unavailable",unavailable); }
    }
}
