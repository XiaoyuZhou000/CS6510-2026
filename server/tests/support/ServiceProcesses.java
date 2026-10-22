package support;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Starts real service JVMs from the built artifacts ({@code server/out/*.jar}, {@code lib/grpc/*},
 * and Connector/J) for process-level crash, restart, and isolation tests (R15).
 *
 * <pre>{@code
 * try (ServiceProcesses.ServiceProcess inventory = ServiceProcesses.service("inventory")
 *         .database(inventorySchema).httpPort(GrpcTestSupport.freePort()).grpcPort(grpcPort).start()) {
 *     inventory.awaitHttp("/items", 200, Duration.ofSeconds(30));
 * }
 * }</pre>
 */
public final class ServiceProcesses {
    private static final Set<String> SERVICES = Set.of("transaction", "inventory", "analytics");
    /** Shared-database variables no service may inherit (R3). */
    private static final List<String> SCRUBBED = List.of("DB_NAME", "DB_USER", "DB_PASSWORD", "DB_POOL_SIZE",
            "DB_POOL_MAX_SIZE", "TRANSACTION_DB_PASSWORD", "INVENTORY_DB_PASSWORD", "ANALYTICS_DB_PASSWORD");

    private ServiceProcesses() { }

    public static Builder service(String service) {
        if (!SERVICES.contains(service)) throw new IllegalArgumentException("Unknown service: " + service);
        return new Builder(service);
    }

    /** The {@code server/} directory, from either {@code server/} or the repository root. */
    public static Path serverRoot() {
        Path cwd = Path.of("").toAbsolutePath().normalize();
        return Files.isRegularFile(cwd.resolve("out/common.jar")) ? cwd : cwd.resolve("server");
    }

    public static final class Builder {
        private final String service;
        private final Map<String, String> environment = new LinkedHashMap<>();
        private final List<String> jvmArguments = new ArrayList<>();
        private int httpPort;

        private Builder(String service) {
            this.service = service;
            environment.put("PERF_METRICS", "false");
        }

        private String prefix() { return service.toUpperCase(java.util.Locale.ROOT); }

        /** Points the service at a disposable schema through its own {@code <SERVICE>_DB_*} variables. */
        public Builder database(TestSchema schema) {
            environment.put("DB_HOST", schema.host());
            environment.put("DB_PORT", Integer.toString(schema.port()));
            environment.put(prefix() + "_DB_NAME", schema.name());
            environment.put(prefix() + "_DB_USER", schema.user());
            environment.put(prefix() + "_DB_PASSWORD", schema.password());
            environment.putIfAbsent(prefix() + "_DB_POOL_SIZE", "4");
            environment.putIfAbsent(prefix() + "_DB_POOL_MAX_SIZE", "8");
            return this;
        }

        public Builder httpPort(int port) {
            this.httpPort = port;
            environment.put(prefix() + "_PORT", Integer.toString(port));
            return this;
        }

        public Builder grpcPort(int port) {
            environment.put(prefix() + "_GRPC_PORT", Integer.toString(port));
            return this;
        }

        /** {@code INVENTORY_GRPC_TARGET} or {@code ANALYTICS_GRPC_TARGET} for the Transaction Service. */
        public Builder grpcTarget(String peer, int port) {
            environment.put(peer.toUpperCase(java.util.Locale.ROOT) + "_GRPC_TARGET", "127.0.0.1:" + port);
            return this;
        }

        public Builder env(String key, String value) {
            environment.put(key, value);
            return this;
        }

        public Builder perfMetrics(boolean enabled) {
            return env("PERF_METRICS", Boolean.toString(enabled));
        }

        /** Arms fault hooks in the child JVM through {@code -Dselfcheckout.faults} (see {@link FaultInjection}). */
        public Builder faults(String specification) {
            if (specification != null && !specification.isBlank()) {
                jvmArguments.add("-Dselfcheckout.faults=" + specification);
            }
            return this;
        }

        public ServiceProcess start() throws IOException {
            Path server = serverRoot();
            String classpath = String.join(File.pathSeparator,
                    server.resolve("out/" + service + ".jar").toString(),
                    server.resolve("out/common.jar").toString(),
                    server.resolve("out/contracts.jar").toString(),
                    server.resolve("lib/grpc").toString() + File.separator + "*",
                    server.resolve("lib/mysql-connector-j-9.0.0.jar").toString());
            String main = service + "." + Character.toUpperCase(service.charAt(0)) + service.substring(1) + "Main";
            List<String> command = new ArrayList<>();
            command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
            command.addAll(jvmArguments);
            command.add("-cp");
            command.add(classpath);
            command.add(main);
            ProcessBuilder builder = new ProcessBuilder(command).directory(server.toFile());
            SCRUBBED.forEach(builder.environment()::remove);
            builder.environment().putAll(environment);
            Path log = Files.createTempFile("selfcheckout-" + service + "-", ".log");
            builder.redirectErrorStream(true);
            builder.redirectOutput(log.toFile());
            return new ServiceProcess(service, builder.start(), log, httpPort);
        }
    }

    /** A running service JVM; {@link #close()} stops it. */
    public static final class ServiceProcess implements AutoCloseable {
        private final String service;
        private final Process process;
        private final Path log;
        private final int httpPort;

        private ServiceProcess(String service, Process process, Path log, int httpPort) {
            this.service = service;
            this.process = process;
            this.log = log;
            this.httpPort = httpPort;
        }

        public String service() { return service; }
        public Process process() { return process; }
        public boolean isAlive() { return process.isAlive(); }
        public int httpPort() { return httpPort; }

        /** Everything the service printed so far, for assertion messages. */
        public String output() {
            try {
                return Files.readString(log, StandardCharsets.UTF_8);
            } catch (IOException unreadable) {
                return "<log unreadable: " + unreadable.getMessage() + ">";
            }
        }

        /** Waits until {@code GET path} on the service's HTTP port returns {@code status}. */
        public void awaitHttp(String path, int status, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                failIfExited();
                if (httpStatus(httpPort, path) == status) return;
                Thread.sleep(100);
            }
            throw new AssertionError(service + " did not answer " + status + " on " + path + " within " + timeout
                    + System.lineSeparator() + output());
        }

        /** Waits until a TCP port accepts connections, for example a gRPC port. */
        public void awaitPort(int port, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                failIfExited();
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
                    return;
                } catch (IOException notYet) {
                    Thread.sleep(100);
                }
            }
            throw new AssertionError(service + " did not listen on " + port + " within " + timeout
                    + System.lineSeparator() + output());
        }

        /** Waits for the process to exit by itself, for example at a {@code halt} fault, and returns its code. */
        public int awaitExit(Duration timeout) throws InterruptedException {
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError(service + " did not exit within " + timeout + System.lineSeparator() + output());
            }
            return process.exitValue();
        }

        /** Stops the service: a normal termination request first, then a forced kill. */
        public void stop() throws InterruptedException {
            if (!process.isAlive()) return;
            process.destroy();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(10, TimeUnit.SECONDS);
            }
        }

        /** Kills the service without letting shutdown hooks run, like a crash. */
        public void kill() throws InterruptedException {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
        }

        private void failIfExited() {
            if (!process.isAlive()) {
                throw new AssertionError(service + " exited with code " + process.exitValue()
                        + System.lineSeparator() + output());
            }
        }

        @Override
        public void close() throws InterruptedException {
            stop();
            try { Files.deleteIfExists(log); } catch (IOException ignored) { }
        }
    }

    /** The HTTP status of {@code GET path} on a local port, or -1 when nothing answers. */
    public static int httpStatus(int port, String path) {
        try {
            HttpURLConnection connection = (HttpURLConnection) URI.create("http://127.0.0.1:" + port + path)
                    .toURL().openConnection();
            connection.setConnectTimeout(500);
            connection.setReadTimeout(2000);
            try {
                return connection.getResponseCode();
            } finally {
                connection.disconnect();
            }
        } catch (IOException unreachable) {
            return -1;
        }
    }
}
