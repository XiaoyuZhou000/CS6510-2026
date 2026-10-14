package integration;

import org.opentest4j.TestAbortedException;
import support.PipelineTestSupport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/** Launches the checked-in Nginx route map on isolated integration-test ports. */
final class PublicRouterTestSupport implements AutoCloseable {
    private final Process process;
    private final Path prefix;
    private final String nginx;
    private final String configFile;
    private final String baseUrl;

    private PublicRouterTestSupport(Process process, Path prefix, String nginx,
            String configFile, String baseUrl) {
        this.process = process;
        this.prefix = prefix;
        this.nginx = nginx;
        this.configFile = configFile;
        this.baseUrl = baseUrl;
    }

    String baseUrl() { return baseUrl; }

    static PublicRouterTestSupport start(int transactionPort, int inventoryPort,
            int analyticsPort) throws Exception {
        String nginx = System.getenv().getOrDefault("NGINX_BIN", "nginx");
        try {
            Process version = new ProcessBuilder(nginx, "-v").start();
            if (!version.waitFor(3, TimeUnit.SECONDS) || version.exitValue() != 0)
                throw new IOException("nginx -v failed");
        } catch (IOException missing) {
            throw new TestAbortedException(
                "Nginx unavailable; set NGINX_BIN to run public-routing integration tests", missing);
        }
        Path template = Files.isRegularFile(Path.of("../deploy/nginx.conf"))
            ? Path.of("../deploy/nginx.conf") : Path.of("deploy/nginx.conf");
        String config = Files.readString(template)
            .replace("listen 8080;", "listen " + PipelineTestSupport.freePort() + ";")
            .replace("server 127.0.0.1:8081;",
                "server 127.0.0.1:" + transactionPort + ";")
            .replace("server 127.0.0.1:8082;",
                "server 127.0.0.1:" + inventoryPort + ";")
            .replace("server 127.0.0.1:8083;",
                "server 127.0.0.1:" + analyticsPort + ";");
        var listener = Pattern.compile("listen (\\d+);").matcher(config);
        if (!listener.find()) throw new AssertionError("Public Nginx config has no listener");
        String base = "http://127.0.0.1:" + listener.group(1);
        Path prefix = Files.createTempDirectory("cs6510-nginx-");
        Files.createDirectories(prefix.resolve("logs"));
        Files.createDirectories(prefix.resolve("temp"));
        Path configFile = prefix.resolve("nginx.conf");
        Files.writeString(configFile, config);
        String nginxPrefix = prefix.toString().replace('\\', '/') + "/";
        String nginxConfig = configFile.toString().replace('\\', '/');
        Process process = new ProcessBuilder(nginx, "-p", nginxPrefix, "-c",
            nginxConfig, "-g", "daemon off;")
            .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        PublicRouterTestSupport router = new PublicRouterTestSupport(
            process, prefix, nginx, nginxConfig, base);
        try {
            PipelineTestSupport.awaitHttpStatus(process, base + "/unknown-route", 404);
            return router;
        } catch (Exception | Error failure) {
            router.close();
            throw failure;
        }
    }

    @Override public void close() throws Exception {
        Process stop = new ProcessBuilder(nginx, "-p", prefix.toString().replace('\\', '/') + "/",
            "-c", configFile, "-s", "stop")
            .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        stop.waitFor(5, TimeUnit.SECONDS);
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
        }
        Files.deleteIfExists(prefix.resolve("nginx.conf"));
        Files.deleteIfExists(prefix.resolve("nginx.pid"));
    }
}
