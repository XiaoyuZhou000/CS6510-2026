package database;

import org.junit.jupiter.api.Test;
import support.TransactionTestSchema;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionPoolGrowthTest {
    @Test
    void growsOnlyWhenBorrowedConnectionsExceedInitialSizeAndWakesWaiter() throws Exception {
        try (TransactionTestSchema schema = TransactionTestSchema.create();
             ConnectionPool pool = new ConnectionPool(schema.host(), schema.port(), schema.name(),
                     schema.user(), schema.password(), 2, 4);
             var worker = Executors.newSingleThreadExecutor()) {
            assertEquals(2, pool.occupancy().size());
            List<Connection> borrowed = new ArrayList<>();
            for (int i = 0; i < 4; i++) borrowed.add(pool.borrow());
            assertEquals(4, pool.occupancy().size());
            assertEquals(4, pool.occupancy().active());
            var waiting = worker.submit(pool::borrow);
            assertThrows(java.util.concurrent.TimeoutException.class,
                () -> waiting.get(100, TimeUnit.MILLISECONDS));
            pool.release(borrowed.removeLast());
            Connection resumed = waiting.get(2, TimeUnit.SECONDS);
            for (Connection connection : borrowed) pool.release(connection);
            pool.release(resumed);
            assertEquals(4, pool.occupancy().idle());
        }
    }
}
