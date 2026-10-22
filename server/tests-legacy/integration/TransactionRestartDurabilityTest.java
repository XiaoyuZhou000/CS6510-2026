package integration;

import api.json.Json;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import support.CheckoutDatabase;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TransactionRestartDurabilityTest {
    private final HttpClient client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    @Test
    void acknowledgedOpenBasketSurvivesProcessRestartAndCompletesExactlyOnce() throws Exception {
        try (CheckoutDatabase db=open()) {
            int port=freePort();
            Process service=start(db,port);
            String base="http://127.0.0.1:"+port;
            try {
                awaitReady(service,base);
                var started=post(base,"/transactions","{\"stationId\":\"restart-station\"}");
                assertEquals(201,started.statusCode(),started.body());
                Map<String,Object> start=Json.parseObject(started.body());
                String id=start.get("transactionId").toString();
                String startedAt=start.get("startedAt").toString();
                assertEquals(0,((Number)start.get("itemCount")).intValue());
                assertEquals(0,((Number)start.get("runningTotal")).doubleValue(),0.001);
                assertEquals(200,post(base,"/transactions/"+id+"/items",
                    "{\"sku\":\""+CheckoutDatabase.SKU+"\"}").statusCode());
                assertEquals(200,post(base,"/transactions/"+id+"/items",
                    "{\"sku\":\""+CheckoutDatabase.SKU+"\"}").statusCode());
                assertEquals(200,post(base,"/transactions/"+id+"/items",
                    "{\"sku\":\""+CheckoutDatabase.OTHER_SKU+"\"}").statusCode());
                assertEquals(10,db.stock());
                assertEquals(10,db.stock(CheckoutDatabase.OTHER_SKU));
                assertEquals(3,db.eventCount());
                service.destroyForcibly();
                assertTrue(service.waitFor(5,TimeUnit.SECONDS));
                service=start(db,port);
                awaitReady(service,base);
                var lookup=get(base,"/transactions/"+id);
                assertEquals(200,lookup.statusCode(),lookup.body());
                var open=Json.parseObject(lookup.body());
                assertEquals("OPEN",open.get("status"));
                assertEquals(startedAt,open.get("startedAt"));
                assertEquals(3,((Number)open.get("itemCount")).intValue());
                assertEquals(6.25,((Number)open.get("runningTotal")).doubleValue(),0.001);
                var completed=post(base,"/transactions/"+id+"/complete","");
                assertEquals(200,completed.statusCode(),completed.body());
                var receipt=Json.parseObject(completed.body());
                assertEquals(3,((Number)receipt.get("itemCount")).intValue());
                assertEquals(6.25,((Number)receipt.get("totalAmount")).doubleValue(),0.001);
                assertEquals(startedAt,receipt.get("startedAt"));
                assertNotNull(receipt.get("completedAt"));
                assertEquals(2,((java.util.List<?>)receipt.get("lines")).size());
                assertEquals(8,db.stock());
                assertEquals(9,db.stock(CheckoutDatabase.OTHER_SKU));
                var duplicate=post(base,"/transactions/"+id+"/complete","");
                assertEquals(409,duplicate.statusCode(),duplicate.body());
                assertEquals("TRANSACTION_NOT_OPEN",Json.parseObject(duplicate.body()).get("error"));
                assertEquals(8,db.stock());
                assertEquals(9,db.stock(CheckoutDatabase.OTHER_SKU));
            } finally {
                service.destroyForcibly();
                service.waitFor(5,TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void publicTransactionErrorsKeepTheirStatusesAndShapes() throws Exception {
        try (CheckoutDatabase db=open()) {
            int port=freePort();
            Process service=start(db,port);
            String base="http://127.0.0.1:"+port;
            try {
                awaitReady(service,base);
                assertError(post(base,"/transactions","{}"),400,"MISSING_STATION_ID");
                assertError(post(base,"/transactions","{\"stationId\":\"\"}"),400,"MISSING_STATION_ID");
                String id=Json.parseObject(post(base,"/transactions",
                    "{\"stationId\":\"error-station\"}").body()).get("transactionId").toString();
                assertError(post(base,"/transactions/missing/items",
                    "{\"sku\":\""+CheckoutDatabase.SKU+"\"}"),404,"TRANSACTION_NOT_FOUND");
                assertError(get(base,"/transactions/missing"),404,"TRANSACTION_NOT_FOUND");
                assertError(post(base,"/transactions/"+id+"/items","{}"),400,"MISSING_SKU");
                assertError(post(base,"/transactions/"+id+"/items",
                    "{\"sku\":\"absent\"}"),404,"SKU_NOT_FOUND");
                assertError(post(base,"/transactions/"+id+"/complete",""),409,"EMPTY_BASKET");
                assertEquals(200,post(base,"/transactions/"+id+"/items",
                    "{\"sku\":\""+CheckoutDatabase.SKU+"\"}").statusCode());
                assertEquals(200,post(base,"/transactions/"+id+"/complete","").statusCode());
                assertError(post(base,"/transactions/"+id+"/items",
                    "{\"sku\":\""+CheckoutDatabase.SKU+"\"}"),409,"TRANSACTION_NOT_OPEN");
                assertError(post(base,"/transactions/"+id+"/complete",""),409,"TRANSACTION_NOT_OPEN");
            } finally {
                service.destroyForcibly();
                service.waitFor(5,TimeUnit.SECONDS);
            }
        }
    }

    private static void assertError(HttpResponse<String> response,int status,String code) {
        assertEquals(status,response.statusCode(),response.body());
        var body=Json.parseObject(response.body());
        assertEquals(java.util.Set.of("error","message"),body.keySet());
        assertEquals(code,body.get("error"));
        assertNotNull(body.get("message"));
    }

    @Test
    void failedDatabaseWriteReturnsInternalErrorWithoutAcknowledgingScan() throws Exception {
        try (CheckoutDatabase db=open()) {
            int port=freePort();
            Process service=start(db,port);
            String base="http://127.0.0.1:"+port;
            try {
                awaitReady(service,base);
                String id=Json.parseObject(post(base,"/transactions",
                    "{\"stationId\":\"failure-station\"}").body()).get("transactionId").toString();
                try (var admin=java.sql.DriverManager.getConnection(
                        "jdbc:mysql://"+db.host()+":"+db.port()+"/"+db.databaseName()+
                        "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                        db.user(),db.password());
                     var ddl=admin.createStatement()) {
                    ddl.executeUpdate("DROP TABLE accepted_scan");
                }
                assertError(post(base,"/transactions/"+id+"/items",
                    "{\"sku\":\""+CheckoutDatabase.SKU+"\"}"),500,"INTERNAL_ERROR");
                var open=Json.parseObject(get(base,"/transactions/"+id).body());
                assertEquals("OPEN",open.get("status"));
                assertEquals(0,((Number)open.get("itemCount")).intValue());
                assertEquals(0,((Number)open.get("runningTotal")).doubleValue(),0.001);
                assertEquals(10,db.stock());
            } finally {
                service.destroyForcibly();
                service.waitFor(5,TimeUnit.SECONDS);
            }
        }
    }

    private static CheckoutDatabase open() throws Exception {
        try { return new CheckoutDatabase(10); }
        catch (java.sql.SQLException unavailable) { throw new TestAbortedException("MySQL unavailable",unavailable); }
    }
    private static int freePort() throws Exception {
        try (ServerSocket socket=new ServerSocket(0)) { return socket.getLocalPort(); }
    }
    private static Process start(CheckoutDatabase db,int port) throws Exception {
        Path root=Files.isRegularFile(Path.of("out/transaction.jar")) ? Path.of(".") : Path.of("server");
        String cp=String.join(java.io.File.pathSeparator,
            root.resolve("out/common.jar").toAbsolutePath().toString(),
            root.resolve("out/transaction.jar").toAbsolutePath().toString(),
            root.resolve("lib/mysql-connector-j-9.0.0.jar").toAbsolutePath().toString());
        String java=Path.of(System.getProperty("java.home"),"bin","java").toString();
        ProcessBuilder builder=new ProcessBuilder(java,"-cp",cp,"transaction.TransactionMain",
            Integer.toString(port));
        builder.environment().put("DB_HOST",db.host());
        builder.environment().put("DB_PORT",Integer.toString(db.port()));
        builder.environment().put("DB_NAME",db.databaseName());
        builder.environment().put("DB_USER",db.user());
        builder.environment().put("DB_PASSWORD",db.password());
        builder.environment().put("DB_POOL_SIZE","4");
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        return builder.start();
    }
    private void awaitReady(Process process,String base) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime()<deadline) {
            if (!process.isAlive()) fail("Transaction Service exited during startup");
            try {
                if (get(base,"/transactions/missing").statusCode()==404) return;
            } catch (java.io.IOException ignored) { }
            Thread.sleep(50);
        }
        fail("Transaction Service did not become ready");
    }
    private HttpResponse<String> post(String base,String path,String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(base+path))
            .header("Content-Type","application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
            HttpResponse.BodyHandlers.ofString());
    }
    private HttpResponse<String> get(String base,String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(base+path)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    }
}




