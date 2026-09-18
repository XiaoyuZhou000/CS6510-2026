package json;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Json {

    private Json() {}

    // ---------------------------------------------------------------
    // Parsing (same parser as mockserver/Json.java)
    // ---------------------------------------------------------------

    public static Object parse(String text) {
        Parser p = new Parser(text);
        p.skipWhitespace();
        Object value = p.parseValue();
        p.skipWhitespace();
        return value;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String text) {
        Object value = parse(text);
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("Expected a JSON object at the root, got: " + value);
        }
        return (Map<String, Object>) value;
    }

    public static String getString(Map<String, Object> obj, String key) {
        Object v = obj.get(key);
        return v == null ? null : v.toString();
    }

    public static double getDouble(Map<String, Object> obj, String key) {
        Object v = obj.get(key);
        if (v == null) throw new IllegalArgumentException("Missing numeric field: " + key);
        return ((Number) v).doubleValue();
    }

    public static int getInt(Map<String, Object> obj, String key) {
        Object v = obj.get(key);
        if (v == null) throw new IllegalArgumentException("Missing numeric field: " + key);
        return ((Number) v).intValue();
    }

    @SuppressWarnings("unchecked")
    public static List<Object> getList(Map<String, Object> obj, String key) {
        Object v = obj.get(key);
        if (v == null) return List.of();
        return (List<Object>) v;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asObject(Object v) {
        return (Map<String, Object>) v;
    }

    // ---------------------------------------------------------------
    // Writing — object/array serialization helpers for all response DTOs
    // ---------------------------------------------------------------

    public static String quote(String raw) {
        if (raw == null) return "null";
        StringBuilder sb = new StringBuilder("\"");
        for (char c : raw.toCharArray()) {
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> sb.append(c);
            }
        }
        return sb.append('"').toString();
    }

    /** Serialize a BigDecimal as a plain decimal string (no scientific notation). */
    public static String number(BigDecimal v) {
        return v.toPlainString();
    }

    /** Serialize a long. */
    public static String number(long v) {
        return Long.toString(v);
    }

    /** Serialize an int. */
    public static String number(int v) {
        return Integer.toString(v);
    }

    /** Build a JSON object string from alternating key/value String pairs. */
    public static String object(String... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Must supply key/value pairs");
        }
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < keyValues.length; i += 2) {
            if (i > 0) sb.append(',');
            sb.append(quote(keyValues[i])).append(':').append(keyValues[i + 1]);
        }
        return sb.append('}').toString();
    }

    /** Build a JSON array string from a collection of already-serialized JSON values. */
    public static String array(Collection<String> elements) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (String e : elements) {
            if (!first) sb.append(',');
            sb.append(e);
            first = false;
        }
        return sb.append(']').toString();
    }

    // ---------------------------------------------------------------
    // Internal parser
    // ---------------------------------------------------------------

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) { this.s = s; this.pos = 0; }

        void skipWhitespace() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
        }

        char peek() { return s.charAt(pos); }

        void expect(char c) {
            if (pos >= s.length() || s.charAt(pos) != c) {
                throw new IllegalArgumentException("Expected '" + c + "' at position " + pos);
            }
            pos++;
        }

        Object parseValue() {
            skipWhitespace();
            char c = peek();
            return switch (c) {
                case '{' -> parseObjectValue();
                case '[' -> parseArrayValue();
                case '"' -> parseStringValue();
                case 't', 'f' -> parseBooleanValue();
                case 'n' -> parseNullValue();
                default -> parseNumberValue();
            };
        }

        Map<String, Object> parseObjectValue() {
            Map<String, Object> map = new LinkedHashMap<>();
            expect('{');
            skipWhitespace();
            if (peek() == '}') { pos++; return map; }
            while (true) {
                skipWhitespace();
                String key = parseStringValue();
                skipWhitespace();
                expect(':');
                Object value = parseValue();
                map.put(key, value);
                skipWhitespace();
                if (peek() == ',') { pos++; continue; }
                expect('}');
                break;
            }
            return map;
        }

        List<Object> parseArrayValue() {
            List<Object> list = new ArrayList<>();
            expect('[');
            skipWhitespace();
            if (peek() == ']') { pos++; return list; }
            while (true) {
                Object value = parseValue();
                list.add(value);
                skipWhitespace();
                if (peek() == ',') { pos++; continue; }
                expect(']');
                break;
            }
            return list;
        }

        String parseStringValue() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = s.charAt(pos++);
                if (c == '"') break;
                if (c == '\\') {
                    char esc = s.charAt(pos++);
                    switch (esc) {
                        case '"'  -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/'  -> sb.append('/');
                        case 'n'  -> sb.append('\n');
                        case 't'  -> sb.append('\t');
                        case 'r'  -> sb.append('\r');
                        case 'b'  -> sb.append('\b');
                        case 'f'  -> sb.append('\f');
                        case 'u'  -> {
                            String hex = s.substring(pos, pos + 4);
                            sb.append((char) Integer.parseInt(hex, 16));
                            pos += 4;
                        }
                        default -> throw new IllegalArgumentException("Unknown escape: \\" + esc);
                    }
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }

        Boolean parseBooleanValue() {
            if (s.startsWith("true", pos))  { pos += 4; return Boolean.TRUE; }
            if (s.startsWith("false", pos)) { pos += 5; return Boolean.FALSE; }
            throw new IllegalArgumentException("Invalid boolean literal at " + pos);
        }

        Object parseNullValue() {
            if (s.startsWith("null", pos)) { pos += 4; return null; }
            throw new IllegalArgumentException("Invalid null literal at " + pos);
        }

        Double parseNumberValue() {
            int start = pos;
            while (pos < s.length() && "-+0123456789.eE".indexOf(s.charAt(pos)) >= 0) pos++;
            return Double.parseDouble(s.substring(start, pos));
        }
    }
}
