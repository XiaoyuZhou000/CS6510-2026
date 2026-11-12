import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small, dependency-free JSON reader and writer for the subscriber test application, in the style of
 * {@code load-client/src/Json.java}. Objects parse to insertion-ordered maps, integral numbers to {@link Long}
 * (seeds, offsets, and epoch milliseconds must stay exact), other numbers to {@link Double}.
 *
 * <p>The writers keep map order, so a caller that builds its maps in a fixed order gets byte-identical output
 * (SC-013). {@link #pretty} puts an array of plain values on one line and every object member on its own line.
 */
public final class Json {
    private Json() { }

    public static Object parse(String text) {
        Parser parser = new Parser(text);
        parser.skipWhitespace();
        Object value = parser.value();
        parser.skipWhitespace();
        if (parser.pos != text.length()) throw new IllegalArgumentException("Trailing text at " + parser.pos);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String text) {
        Object value = parse(text);
        if (!(value instanceof Map)) throw new IllegalArgumentException("Expected a JSON object");
        return (Map<String, Object>) value;
    }

    public static String getString(Map<String, Object> object, String key) {
        Object value = object.get(key);
        return value == null ? null : value.toString();
    }

    public static long getLong(Map<String, Object> object, String key) {
        Object value = object.get(key);
        if (!(value instanceof Number number)) throw new IllegalArgumentException("Missing numeric field: " + key);
        return number.longValue();
    }

    public static boolean getBoolean(Map<String, Object> object, String key) {
        return Boolean.TRUE.equals(object.get(key));
    }

    @SuppressWarnings("unchecked")
    public static List<Object> getList(Map<String, Object> object, String key) {
        Object value = object.get(key);
        return value == null ? List.of() : (List<Object>) value;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> getObject(Map<String, Object> object, String key) {
        Object value = object.get(key);
        return value == null ? null : (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asObject(Object value) {
        if (!(value instanceof Map)) throw new IllegalArgumentException("Expected a JSON object, got: " + value);
        return (Map<String, Object>) value;
    }

    public static List<String> strings(List<Object> values) {
        List<String> result = new ArrayList<>(values.size());
        for (Object value : values) result.add(String.valueOf(value));
        return result;
    }

    // ------------------------------------------------------------------ writing

    public static String quote(String raw) {
        StringBuilder text = new StringBuilder(raw.length() + 2).append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"' -> text.append("\\\"");
                case '\\' -> text.append("\\\\");
                case '\n' -> text.append("\\n");
                case '\r' -> text.append("\\r");
                case '\t' -> text.append("\\t");
                default -> {
                    if (c < 0x20) text.append(String.format("\\u%04x", (int) c));
                    else text.append(c);
                }
            }
        }
        return text.append('"').toString();
    }

    /** One line, no spaces. */
    public static String compact(Object value) {
        StringBuilder text = new StringBuilder();
        write(text, value, -1, 0);
        return text.toString();
    }

    /** Indented by two spaces, ending with a newline. */
    public static String pretty(Object value) {
        StringBuilder text = new StringBuilder();
        write(text, value, 2, 0);
        return text.append('\n').toString();
    }

    private static void write(StringBuilder text, Object value, int indent, int depth) {
        if (value == null) {
            text.append("null");
        } else if (value instanceof String s) {
            text.append(quote(s));
        } else if (value instanceof Boolean b) {
            text.append(b);
        } else if (value instanceof Double || value instanceof Float) {
            double d = ((Number) value).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) text.append("null");
            else text.append(BigDecimal.valueOf(d).stripTrailingZeros().toPlainString());
        } else if (value instanceof Number n) {
            text.append(n.longValue());
        } else if (value instanceof Map<?, ?> map) {
            writeObject(text, map, indent, depth);
        } else if (value instanceof List<?> list) {
            writeArray(text, list, indent, depth);
        } else {
            text.append(quote(value.toString()));
        }
    }

    private static void writeObject(StringBuilder text, Map<?, ?> map, int indent, int depth) {
        if (map.isEmpty()) {
            text.append("{}");
            return;
        }
        text.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) text.append(',');
            first = false;
            newline(text, indent, depth + 1);
            text.append(quote(String.valueOf(entry.getKey()))).append(indent < 0 ? ":" : ": ");
            write(text, entry.getValue(), indent, depth + 1);
        }
        newline(text, indent, depth);
        text.append('}');
    }

    private static void writeArray(StringBuilder text, List<?> list, int indent, int depth) {
        if (list.isEmpty()) {
            text.append("[]");
            return;
        }
        boolean plain = true;
        for (Object element : list) {
            if (element instanceof Map || element instanceof List) { plain = false; break; }
        }
        text.append('[');
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) text.append(plain && indent >= 0 ? ", " : ",");
            if (!plain) newline(text, indent, depth + 1);
            write(text, list.get(i), indent, depth + 1);
        }
        if (!plain) newline(text, indent, depth);
        text.append(']');
    }

    private static void newline(StringBuilder text, int indent, int depth) {
        if (indent < 0) return;
        text.append('\n');
        for (int i = 0; i < indent * depth; i++) text.append(' ');
    }

    /** An insertion-ordered map built from key/value pairs, for compact report and receipt construction. */
    public static Map<String, Object> object(Object... keyValues) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) map.put((String) keyValues[i], keyValues[i + 1]);
        return map;
    }

    // ------------------------------------------------------------------ parsing

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) { this.s = s; }

        void skipWhitespace() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
        }

        char peek() {
            if (pos >= s.length()) throw new IllegalArgumentException("Unexpected end of JSON");
            return s.charAt(pos);
        }

        void expect(char c) {
            if (pos >= s.length() || s.charAt(pos) != c) {
                throw new IllegalArgumentException("Expected '" + c + "' at position " + pos);
            }
            pos++;
        }

        Object value() {
            skipWhitespace();
            return switch (peek()) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't' -> literal("true", Boolean.TRUE);
                case 'f' -> literal("false", Boolean.FALSE);
                case 'n' -> literal("null", null);
                default -> number();
            };
        }

        Map<String, Object> object() {
            Map<String, Object> map = new LinkedHashMap<>();
            expect('{');
            skipWhitespace();
            if (peek() == '}') { pos++; return map; }
            while (true) {
                skipWhitespace();
                String key = string();
                skipWhitespace();
                expect(':');
                map.put(key, value());
                skipWhitespace();
                if (peek() == ',') { pos++; continue; }
                expect('}');
                return map;
            }
        }

        List<Object> array() {
            List<Object> list = new ArrayList<>();
            expect('[');
            skipWhitespace();
            if (peek() == ']') { pos++; return list; }
            while (true) {
                list.add(value());
                skipWhitespace();
                if (peek() == ',') { pos++; continue; }
                expect(']');
                return list;
            }
        }

        String string() {
            expect('"');
            StringBuilder text = new StringBuilder();
            while (true) {
                char c = s.charAt(pos++);
                if (c == '"') return text.toString();
                if (c != '\\') { text.append(c); continue; }
                char escaped = s.charAt(pos++);
                switch (escaped) {
                    case '"' -> text.append('"');
                    case '\\' -> text.append('\\');
                    case '/' -> text.append('/');
                    case 'n' -> text.append('\n');
                    case 't' -> text.append('\t');
                    case 'r' -> text.append('\r');
                    case 'b' -> text.append('\b');
                    case 'f' -> text.append('\f');
                    case 'u' -> { text.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16)); pos += 4; }
                    default -> throw new IllegalArgumentException("Unknown escape \\" + escaped);
                }
            }
        }

        Object literal(String word, Object value) {
            if (!s.startsWith(word, pos)) throw new IllegalArgumentException("Invalid literal at " + pos);
            pos += word.length();
            return value;
        }

        Object number() {
            int start = pos;
            while (pos < s.length() && "-+0123456789.eE".indexOf(s.charAt(pos)) >= 0) pos++;
            String token = s.substring(start, pos);
            if (token.isEmpty()) throw new IllegalArgumentException("Invalid JSON value at " + start);
            if (token.indexOf('.') < 0 && token.indexOf('e') < 0 && token.indexOf('E') < 0) {
                try {
                    return Long.parseLong(token);
                } catch (NumberFormatException tooLarge) {
                    return Double.parseDouble(token);
                }
            }
            return Double.parseDouble(token);
        }
    }
}
