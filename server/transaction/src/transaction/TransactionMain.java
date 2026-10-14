package transaction;

import api.TransactionHttpHandler;
import com.sun.net.httpserver.HttpServer;
import database.ConnectionPool;
import database.JdbcAcceptedScanStore;
import database.JdbcCheckoutCompletionStore;
import database.JdbcTransactionStore;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/** Independent Transaction Service entry point. */
public final class TransactionMain {
    private TransactionMain() { }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : intEnv("TRANSACTION_PORT", 8081);
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(env("DB_HOST","127.0.0.1"),intEnv("DB_PORT",3307),
                env("DB_NAME","cs6510_selfcheckout"),env("DB_USER","root"),
                env("DB_PASSWORD",""),intEnv("DB_POOL_SIZE",10));
        } catch (Exception failure) {
            System.err.println("Transaction Service cannot connect to MySQL: " + failure.getMessage());
            throw failure;
        }
        HttpServer server = HttpServer.create(new InetSocketAddress(port),0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        var service = new TransactionService(new JdbcTransactionStore(pool),
            new JdbcAcceptedScanStore(pool),new JdbcCheckoutCompletionStore(pool));
        server.createContext("/transactions",new TransactionHttpHandler(service));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(5);
            pool.close();
        }));
        server.start();
        System.out.println("Transaction Service listening on port " + port);
    }

    private static String env(String key,String fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : value;
    }
    private static int intEnv(String key,int fallback) {
        String value = System.getenv(key);
        return value == null || value.isEmpty() ? fallback : Integer.parseInt(value);
    }
}
