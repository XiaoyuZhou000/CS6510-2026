package performance;

import database.JdbcCheckoutOperationStore;
import inventory.CheckoutStockService;
import org.junit.jupiter.api.Test;
import support.CountingConnections;
import support.InventoryTestSchema;
import support.StatementBudgets;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Inventory's ApplyCheckout in a fixed number of round trips, whatever the basket size (R8, R22): the
 * outcome row and the stock UPDATE share one commit, and a replay or a rejection issues no stock UPDATE.
 */
class InventoryApplyStatementBudgetTest {

    @Test
    void appliedAndRejectedAppliesStayWithinTheirBudgetsForEveryBasketSize() throws Exception {
        CountingConnections counting = new CountingConnections();
        try (InventoryTestSchema schema = InventoryTestSchema.create()) {
            schema.seed(40, 100);
            CheckoutStockService stock = new CheckoutStockService(new JdbcCheckoutOperationStore(
                    schema.newPool("test.apply.budget." + UUID.randomUUID().toString().substring(0, 8), 2, 4, counting::wrap)));
            for (int size : List.of(1, 5, 20)) {
                List<CheckoutStockService.Line> lines = lines(size, 1);
                String tx = UUID.randomUUID().toString();
                try (CountingConnections.Scope scope = counting.scope()) {
                    assertEquals(CheckoutStockService.Outcome.APPLIED, stock.apply(tx, 1, lines).outcome());
                    CountingConnections.Counts counts = scope.counts();
                    assertEquals(1, counts.selects(), size + " lines: one locking SELECT");
                    assertEquals(1, counts.inserts(), size + " lines: one outcome INSERT");
                    assertEquals(1, counts.updates(), size + " lines: one CASE UPDATE");
                    assertEquals(1, counts.commits(), size + " lines: the outcome and the deduction share one commit");
                    StatementBudgets.assertWithin("inventory.apply.applied.statements", counts.statements());
                    StatementBudgets.assertWithin("inventory.apply.applied.commits", counts.commits());
                }
                try (CountingConnections.Scope scope = counting.scope()) {
                    assertTrue(stock.apply(tx, 1, lines).replayed());
                    assertEquals(0, scope.counts().updates(), "a replay issues no stock UPDATE");
                    assertEquals(0, scope.counts().commits(), "and commits nothing");
                }

                String rejectedTx = UUID.randomUUID().toString();
                List<CheckoutStockService.Line> tooMany = lines(size, 1_000);
                try (CountingConnections.Scope scope = counting.scope()) {
                    assertEquals(CheckoutStockService.Outcome.INSUFFICIENT_STOCK,
                            stock.apply(rejectedTx, 1, tooMany).outcome());
                    CountingConnections.Counts counts = scope.counts();
                    assertEquals(0, counts.updates(), size + " lines: a rejection issues no stock UPDATE");
                    assertEquals(1, counts.selects());
                    assertEquals(1, counts.inserts(), "the rejection is recorded");
                    StatementBudgets.assertWithin("inventory.apply.rejected.statements", counts.statements());
                    StatementBudgets.assertWithin("inventory.apply.rejected.commits", counts.commits());
                }
            }
            assertEquals(97, schema.stock(InventoryTestSchema.sku(1)), "in all three applied baskets, deducted once each");
            assertEquals(98, schema.stock(InventoryTestSchema.sku(2)), "in the 5- and 20-line baskets");
            assertEquals(99, schema.stock(InventoryTestSchema.sku(20)), "in the 20-line basket only");
            assertEquals(100, schema.stock(InventoryTestSchema.sku(21)), "never referenced");
        }
    }

    private static List<CheckoutStockService.Line> lines(int size, int quantity) {
        List<CheckoutStockService.Line> lines = new ArrayList<>(size);
        for (int n = 1; n <= size; n++) lines.add(new CheckoutStockService.Line(InventoryTestSchema.sku(n), quantity));
        return lines;
    }
}
