package unit.transaction;

import database.AcceptedScanStore;
import database.CheckoutCompletionStore;
import database.TransactionStore;
import org.junit.jupiter.api.Test;
import transaction.TransactionOperations;
import transaction.TransactionService;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class TransactionBoundaryIsolationTest {
    @Test
    void publicBusinessContractExposesNoHttpOrJdbcTypes() {
        for (Method method:TransactionOperations.class.getDeclaredMethods()) {
            assertBoundaryType(method.getReturnType());
            for (Class<?> parameter:method.getParameterTypes()) assertBoundaryType(parameter);
        }
        for (Class<?> nested:TransactionOperations.class.getDeclaredClasses()) {
            for (var component:nested.isRecord()?nested.getRecordComponents():new java.lang.reflect.RecordComponent[0])
                assertBoundaryType(component.getType());
        }
    }
    @Test
    void serviceUsesOnlyDurableStoreBoundariesWithoutProcessBasketAuthority() {
        var constructors=TransactionService.class.getConstructors();
        assertEquals(1,constructors.length);
        assertArrayEquals(new Class<?>[]{TransactionStore.class,AcceptedScanStore.class,
            CheckoutCompletionStore.class},constructors[0].getParameterTypes());
        for (var field:TransactionService.class.getDeclaredFields()) {
            assertTrue(Modifier.isFinal(field.getModifiers()));
            assertFalse(Map.class.isAssignableFrom(field.getType()));
            assertFalse(ConcurrentHashMap.class.isAssignableFrom(field.getType()));
            assertNotEquals("transaction.AcceptedScanSink",field.getType().getName());
        }
    }
    private static void assertBoundaryType(Class<?> type) {
        String name=type.getName();
        assertFalse(name.startsWith("com.sun.net.httpserver"),name);
        assertFalse(name.startsWith("java.sql"),name);
    }
}
