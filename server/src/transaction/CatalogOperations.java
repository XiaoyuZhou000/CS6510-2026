package transaction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;

/** API-facing catalog query contract. */
public interface CatalogOperations {

    List<CatalogItemView> listItems();

    record CatalogItemView(String sku, String name, BigDecimal price) {
        public CatalogItemView {
            sku = requiredText(sku, 20, "sku");
            name = requiredText(name, 100, "name");
            Objects.requireNonNull(price, "price must not be null");
            try {
                price = price.setScale(2, RoundingMode.UNNECESSARY);
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException("price must have at most two decimal places", e);
            }
            if (price.signum() < 0 || price.precision() > 10) {
                throw new IllegalArgumentException("price must be a non-negative DECIMAL(10,2)");
            }
        }
    }

    private static String requiredText(String value, int maximumLength, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        if (value.length() > maximumLength) {
            throw new IllegalArgumentException(field + " must be at most " + maximumLength + " characters");
        }
        return value;
    }
}
