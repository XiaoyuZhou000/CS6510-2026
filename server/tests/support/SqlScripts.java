package support;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Runs the version-controlled {@code db/<service>/*.sql} files statement by statement over JDBC. */
public final class SqlScripts {
    private SqlScripts() { }

    /** A file under the repository's {@code db/} directory, from {@code server/} or the repository root. */
    public static Path dbFile(String relative) {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        for (Path root : List.of(cwd.resolve("db"), cwd.resolve("../db"))) {
            Path file = root.resolve(relative).normalize();
            if (Files.isRegularFile(file)) return file;
        }
        throw new IllegalStateException("db/" + relative + " not found from " + cwd);
    }

    public static void run(Connection connection, Path script) throws Exception {
        for (String statement : split(Files.readString(script, StandardCharsets.UTF_8))) {
            try (Statement sql = connection.createStatement()) {
                sql.execute(statement);
            } catch (SQLException failure) {
                throw new SQLException(script.getFileName() + ": " + failure.getMessage() + " in: " + statement,
                        failure.getSQLState(), failure.getErrorCode(), failure);
            }
        }
    }

    /**
     * Splits on top-level semicolons, dropping {@code --}, {@code #}, and block comments and keeping
     * quoted text intact. {@code DELIMITER} blocks are not supported; schema files do not use them.
     */
    public static List<String> split(String script) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int length = script.length();
        for (int i = 0; i < length; i++) {
            char c = script.charAt(i);
            char next = i + 1 < length ? script.charAt(i + 1) : '\0';
            if ((c == '-' && next == '-' && (i + 2 >= length || Character.isWhitespace(script.charAt(i + 2))))
                    || c == '#') {
                while (i < length && script.charAt(i) != '\n') i++;
                current.append('\n');
            } else if (c == '/' && next == '*') {
                int end = script.indexOf("*/", i + 2);
                if (end < 0) throw new IllegalArgumentException("Unterminated block comment");
                i = end + 1;
                current.append(' ');
            } else if (c == '\'' || c == '"' || c == '`') {
                int start = i;
                for (i++; i < length; i++) {
                    char q = script.charAt(i);
                    if (q == '\\' && c != '`') { i++; continue; }
                    if (q == c) {
                        if (i + 1 < length && script.charAt(i + 1) == c) { i++; continue; }
                        break;
                    }
                }
                if (i >= length) throw new IllegalArgumentException("Unterminated quote starting at " + start);
                current.append(script, start, i + 1);
            } else if (c == ';') {
                add(statements, current);
            } else {
                current.append(c);
            }
        }
        add(statements, current);
        return statements;
    }

    private static void add(List<String> statements, StringBuilder current) {
        String statement = current.toString().trim();
        if (statement.toUpperCase(java.util.Locale.ROOT).startsWith("DELIMITER")) {
            throw new IllegalArgumentException("DELIMITER is not supported by SqlScripts");
        }
        if (!statement.isEmpty()) statements.add(statement);
        current.setLength(0);
    }
}
