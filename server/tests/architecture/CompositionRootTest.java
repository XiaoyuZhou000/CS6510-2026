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

/** Focused checks for construction-only bootstrap and handler injection. */
class CompositionRootTest {
    @Test
    void mainContainsConstructionAndRegistrationButNoRequestTimeStoreCalls() throws Exception {
        String source = Files.readString(source("api/Main.java"));
        assertTrue(source.contains("HttpServer.create"));
        assertTrue(source.contains("createContext("));
        assertTrue(source.contains("newVirtualThreadPerTaskExecutor"));
        assertFalse(source.contains(".loadAll("));
        assertFalse(source.contains(".findById("));
        assertFalse(source.contains(".findLowStock("));
        assertFalse(source.contains(".completeAtomically("));
        assertFalse(source.contains(".readLatest("));
        assertFalse(source.contains(".writeWindow("));
    }

    @Test
    void everyHttpHandlerRetainsOnlyBusinessInterfaces() throws Exception {
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

    private static boolean isBusinessInterface(Class<?> type) {
        Package owner = type.getPackage();
        String packageName = owner == null ? "" : owner.getName();
        return type.isInterface() && (packageName.equals("transaction") || packageName.equals("analytics"));
    }

    private static Path source(String relative) {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        Path direct = cwd.resolve("src").resolve(relative);
        return Files.exists(direct) ? direct : cwd.resolve("server/src").resolve(relative);
    }
}
