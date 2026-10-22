package rpc;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * One reused plaintext channel per target per process (R2). A channel is created on first use and
 * connects lazily, so a peer that is down never prevents startup.
 */
public final class GrpcChannels {
    private static final ConcurrentHashMap<String, ManagedChannel> CHANNELS = new ConcurrentHashMap<>();

    private GrpcChannels() { }

    /** The shared channel for {@code host:port}, created once. */
    public static ManagedChannel forTarget(String target) {
        if (target == null || target.isBlank()) throw new IllegalArgumentException("gRPC target is required");
        return CHANNELS.computeIfAbsent(target.trim(),
                address -> NettyChannelBuilder.forTarget(address).usePlaintext().build());
    }

    /** Shuts every channel down, waiting briefly for in-flight calls before forcing it closed. */
    public static void shutdownAll() {
        List<ManagedChannel> channels = new ArrayList<>(CHANNELS.values());
        CHANNELS.clear();
        for (ManagedChannel channel : channels) channel.shutdown();
        for (ManagedChannel channel : channels) {
            try {
                if (!channel.awaitTermination(2, TimeUnit.SECONDS)) channel.shutdownNow();
            } catch (InterruptedException interrupted) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
