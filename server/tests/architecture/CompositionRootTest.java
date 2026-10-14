package architecture;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks each independent composition root and handler injection boundary. */
class CompositionRootTest {
    @Test
    void eachDomainMainConstructsOnlyItsOwnRoutesAndStores() throws Exception {
        for (String domain : List.of("transaction", "inventory", "analytics")) {
            Path main = mainSource(domain);
            String source = Files.readString(main);
            assertTrue(source.contains("HttpServer.create"), main.toString());
            assertTrue(source.contains("createContext("), main.toString());
            assertTrue(source.contains("newVirtualThreadPerTaskExecutor"), main.toString());
            assertTrue(source.contains("new ConnectionPool("), main.toString());
            assertTrue(source.contains("addShutdownHook"), main.toString());
            assertFalse(source.contains(".loadAll("), main.toString());
            assertFalse(source.contains(".findById("), main.toString());
            assertFalse(source.contains(".findLowStock("), main.toString());
            assertFalse(source.contains(".completeAtomically("), main.toString());
            assertFalse(source.contains(".readLatest("), main.toString());
            assertFalse(source.contains(".writeWindow("), main.toString());
            for (String peer : List.of("transaction", "inventory", "analytics")) {
                if (!peer.equals(domain)) {
                    assertFalse(source.matches("(?s).*import\\s+" + peer + "\\..*"),
                            main + " imports peer domain " + peer);
                }
            }
        }
    }

    @Test
    void everyHttpHandlerRetainsOnlyBusinessInterfaces() {
        List<String> violations = new ArrayList<>();
        for (Class<?> handler : List.of(
                api.TransactionHttpHandler.class, api.CatalogHttpHandler.class,
                api.InventoryHttpHandler.class, api.AnalyticsHttpHandler.class)) {
            for (Field field : handler.getDeclaredFields()) {
                if (!isBusinessInterface(field.getType())) {
                    violations.add(handler.getName() + " field " + field.getName()
                            + " is not a business interface: " + field.getType().getName());
                }
            }
            for (Constructor<?> constructor : handler.getDeclaredConstructors()) {
                for (Class<?> parameter : constructor.getParameterTypes()) {
                    if (!isBusinessInterface(parameter)) {
                        violations.add(handler.getName() + " constructor receives " + parameter.getName());
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join(System.lineSeparator(), violations));
    }

    @Test
    void analyticsCompositionBuildsTheFacadeAndDelegatesShutdown() throws Exception {
        String source = Files.readString(mainSource("analytics"));
        assertTrue(source.contains("new AnalyticsService("));
        assertFalse(source.contains("new WindowFilter("));
        assertFalse(source.contains("new RankingFilter("));
        assertFalse(source.contains("new PersistenceFilter("));
        assertTrue(source.contains("running.shutdown()"));
        String facade = Files.readString(serverRoot().resolve("analytics/src/analytics/AnalyticsService.java"));
        assertTrue(facade.contains("shutdownTimeoutMillis <= 0"));
    }

    private static boolean isBusinessInterface(Class<?> type) {
        Package owner = type.getPackage();
        String packageName = owner == null ? "" : owner.getName();
        return type.isInterface() && (packageName.equals("transaction")
                || packageName.equals("inventory") || packageName.equals("analytics"));
    }

    private static Path mainSource(String domain) {
        String name = Character.toUpperCase(domain.charAt(0)) + domain.substring(1) + "Main.java";
        return serverRoot().resolve(domain + "/src/" + domain + "/" + name);
    }

    private static Path serverRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isDirectory(cwd.resolve("server/common")) ? cwd.resolve("server") : cwd;
    }
}