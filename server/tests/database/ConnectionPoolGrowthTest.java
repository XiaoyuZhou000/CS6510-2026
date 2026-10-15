package database;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionPoolGrowthTest {
    @Test
    void growsOnlyWhenBorrowedConnectionsExceedInitialSizeAndWakesWaiter() throws Exception {
        String host = System.getenv().getOrDefault("DB_HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("DB_PORT", "3307"));
        String name = System.getenv().getOrDefault("DB_NAME", "cs6510_selfcheckout");
        String user = System.getenv().getOrDefault("DB_USER", "root");
        String password = System.getenv().getOrDefault("DB_PASSWORD", "");
        try (ConnectionPool pool = new ConnectionPool(host, port, name, user, password, 2, 4);
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
