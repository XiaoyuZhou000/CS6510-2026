package unit.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import rpc.FaultHooks;
import support.FaultInjection;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FaultHooksTest {
    @AfterEach
    void clearProperty() {
        System.clearProperty(FaultHooks.PROPERTY);
    }

    @Test
    void everyHookIsInertWithoutTheProperty() {
        System.clearProperty(FaultHooks.PROPERTY);
        assertDoesNotThrow(() -> FaultHooks.hit("transaction.scan.afterCommit"));
        assertDoesNotThrow(() -> FaultHooks.hit("anything"));
    }

    @Test
    void parsesHaltDelayAndThrowActions() {
        Map<String, FaultHooks.Action> actions =
                FaultHooks.parse(" relay.afterAck:halt, inventory.apply.beforeCommit:delay:250 ,analytics.ingest.afterCommit:throw");
        assertEquals(3, actions.size());
        assertEquals(new FaultHooks.Action(FaultHooks.Kind.HALT, 0), actions.get("relay.afterAck"));
        assertEquals(new FaultHooks.Action(FaultHooks.Kind.DELAY, 250), actions.get("inventory.apply.beforeCommit"));
        assertEquals(new FaultHooks.Action(FaultHooks.Kind.THROW, 0), actions.get("analytics.ingest.afterCommit"));
        assertTrue(FaultHooks.parse("").isEmpty());
        for (String malformed : new String[]{"nohook", ":halt", "x:", "x:explode", "x:delay:", "x:delay:-1",
                "x:delay:soon", "x:halt,x:throw"}) {
            assertThrows(IllegalArgumentException.class, () -> FaultHooks.parse(malformed), malformed);
        }
    }

    @Test
    void throwAndDelayFireOnlyForTheirOwnHook() {
        System.setProperty(FaultHooks.PROPERTY, "test.fail:throw,test.slow:delay:150");
        FaultHooks.InjectedFault fault = assertThrows(FaultHooks.InjectedFault.class, () -> FaultHooks.hit("test.fail"));
        assertTrue(fault.getMessage().contains("test.fail"));
        long started = System.nanoTime();
        FaultHooks.hit("test.slow");
        assertTrue(System.nanoTime() - started >= 140_000_000L, "delay hook sleeps");
        assertDoesNotThrow(() -> FaultHooks.hit("test.other"));
    }

    @Test
    void reparsesWhenThePropertyChanges() {
        System.setProperty(FaultHooks.PROPERTY, "test.hook:throw");
        assertThrows(FaultHooks.InjectedFault.class, () -> FaultHooks.hit("test.hook"));
        System.setProperty(FaultHooks.PROPERTY, "test.elsewhere:throw");
        assertDoesNotThrow(() -> FaultHooks.hit("test.hook"));
        assertThrows(FaultHooks.InjectedFault.class, () -> FaultHooks.hit("test.elsewhere"));
        System.clearProperty(FaultHooks.PROPERTY);
        assertDoesNotThrow(() -> FaultHooks.hit("test.elsewhere"));
    }

    @Test
    void faultInjectionBuildsValidSpecificationsAndRestoresTheProperty() {
        String spec = FaultInjection.spec().halt("relay.afterAck").delay("a.b", 10).fail("c.d").build();
        assertEquals("relay.afterAck:halt,a.b:delay:10,c.d:throw", spec);
        assertThrows(IllegalArgumentException.class, () -> FaultInjection.spec().fail("x").halt("x"));
        System.setProperty(FaultHooks.PROPERTY, "previous.hook:throw");
        try (FaultInjection.Armed armed = FaultInjection.spec().fail("armed.hook").armInThisJvm()) {
            assertThrows(FaultHooks.InjectedFault.class, () -> FaultHooks.hit("armed.hook"));
        }
        assertEquals("previous.hook:throw", System.getProperty(FaultHooks.PROPERTY));
    }
}
