package rpc;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Starts gRPC servers whose handlers run on virtual threads (R2). */
public final class GrpcServers {
    private GrpcServers() { }

    public static Running start(int port, BindableService... services) throws IOException {
        if (services.length == 0) throw new IllegalArgumentException("at least one gRPC service is required");
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        NettyServerBuilder builder = NettyServerBuilder.forPort(port).executor(executor);
        for (BindableService service : services) builder.addService(service);
        try {
            return new Running(builder.build().start(), executor);
        } catch (IOException | RuntimeException failure) {
            executor.shutdownNow();
            throw failure;
        }
    }

    /** A started server; {@link #stop} refuses new calls, drains, then releases the executor. */
    public static final class Running {
        private final Server server;
        private final ExecutorService executor;

        private Running(Server server, ExecutorService executor) {
            this.server = server;
            this.executor = executor;
        }

        public int port() { return server.getPort(); }

        public void stop(Duration timeout) {
            server.shutdown();
            try {
                if (!server.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    server.shutdownNow();
                    server.awaitTermination(1, TimeUnit.SECONDS);
                }
            } catch (InterruptedException interrupted) {
                server.shutdownNow();
                Thread.currentThread().interrupt();
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
