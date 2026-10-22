package support;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** In-process gRPC servers and channels for contract tests, plus free local ports (R2, R15). */
public final class GrpcTestSupport {
    private GrpcTestSupport() { }

    /** Starts {@code services} on a uniquely named in-process server whose handlers run on virtual threads. */
    public static InProcess inProcess(BindableService... services) throws IOException {
        String name = "selfcheckout-test-" + UUID.randomUUID();
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        InProcessServerBuilder builder = InProcessServerBuilder.forName(name).executor(executor);
        for (BindableService service : services) builder.addService(service);
        Server server = builder.build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        return new InProcess(server, channel, executor);
    }

    /** A free TCP port on the loopback interface at the time of the call. */
    public static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    public static final class InProcess implements AutoCloseable {
        private final Server server;
        private final ManagedChannel channel;
        private final ExecutorService executor;

        private InProcess(Server server, ManagedChannel channel, ExecutorService executor) {
            this.server = server;
            this.channel = channel;
            this.executor = executor;
        }

        public ManagedChannel channel() { return channel; }

        @Override
        public void close() throws InterruptedException {
            channel.shutdownNow();
            server.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.awaitTermination(5, TimeUnit.SECONDS);
            executor.shutdownNow();
        }
    }
}
