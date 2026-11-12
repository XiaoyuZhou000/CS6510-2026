package database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

/**
 * The canonical JSON text of Notification's stored lists (007 data-model.md section 2): SKU arrays
 * ({@code subscription_change.added_skus}, {@code removed_skus}, {@code resulting_skus}) are sorted, duplicate-free
 * arrays of strings, and a sale's {@code lines} are {@code [{"sku":"A","quantity":3}]} in SKU order. The same set
 * always gives the same text, so the history can be compared byte for byte.
 */
public final class NotificationJson {
    private NotificationJson() { }

    /** A SKU line of a sale. */
    public record Line(String sku, int quantity) { }

    /** {@code ["A","B"]}: the SKUs sorted and without duplicates. */
    public static String skuArray(Collection<String> skus) {
        StringBuilder text = new StringBuilder("[");
        boolean first = true;
        for (String sku : new TreeSet<>(skus)) {
            if (!first) text.append(',');
            quote(text, sku);
            first = false;
        }
        return text.append(']').toString();
    }

    /** The SKUs of a {@link #skuArray} text, in its order. */
    public static List<String> parseSkuArray(String text) {
        Reader reader = new Reader(text);
        List<String> skus = new ArrayList<>();
        reader.expect('[');
        if (reader.peek() == ']') {
            reader.expect(']');
            reader.end();
            return List.of();
        }
        while (true) {
            skus.add(reader.string());
            char next = reader.next();
            if (next == ']') break;
            if (next != ',') throw new IllegalArgumentException("Malformed SKU array: " + text);
        }
        reader.end();
        return List.copyOf(skus);
    }

    /** {@code [{"sku":"A","quantity":3},...]} in the given order, which callers keep ascending by SKU. */
    public static String lines(List<Line> lines) {
        StringBuilder text = new StringBuilder("[");
        for (int i = 0; i < lines.size(); i++) {
            if (i > 0) text.append(',');
            text.append("{\"sku\":");
            quote(text, lines.get(i).sku());
            text.append(",\"quantity\":").append(lines.get(i).quantity()).append('}');
        }
        return text.append(']').toString();
    }

    private static void quote(StringBuilder text, String value) {
        text.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') text.append('\\').append(c);
            else if (c < 0x20) text.append(String.format("\\u%04x", (int) c));
            else text.append(c);
        }
        text.append('"');
    }

    private static final class Reader {
        private final String text;
        private int position;

        Reader(String text) {
            if (text == null) throw new IllegalArgumentException("SKU array is null");
            this.text = text;
        }

        char peek() {
            skipSpace();
            if (position >= text.length()) throw new IllegalArgumentException("Truncated SKU array: " + text);
            return text.charAt(position);
        }

        char next() {
            char c = peek();
            position++;
            return c;
        }

        void expect(char expected) {
            if (next() != expected) throw new IllegalArgumentException("Malformed SKU array: " + text);
        }

        void end() {
            skipSpace();
            if (position != text.length()) throw new IllegalArgumentException("Trailing text in SKU array: " + text);
        }

        String string() {
            expect('"');
            StringBuilder value = new StringBuilder();
            while (true) {
                if (position >= text.length()) throw new IllegalArgumentException("Unterminated string: " + text);
                char c = text.charAt(position++);
                if (c == '"') return value.toString();
                if (c != '\\') {
                    value.append(c);
                    continue;
                }
                if (position >= text.length()) throw new IllegalArgumentException("Unterminated escape: " + text);
                char escaped = text.charAt(position++);
                if (escaped == 'u') {
                    if (position + 4 > text.length()) throw new IllegalArgumentException("Short escape: " + text);
                    value.append((char) Integer.parseInt(text.substring(position, position + 4), 16));
                    position += 4;
                } else {
                    value.append(escaped);
                }
            }
        }

        private void skipSpace() {
            while (position < text.length() && Character.isWhitespace(text.charAt(position))) position++;
        }
    }
}
