import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One subscriber's receipt file, {@code receipts/<subscriberId>.jsonl} (FR-021, data-model.md section 5): one JSON
 * object per received channel record. A record whose notification identity was already recorded is marked
 * {@code redelivery: true}. A record that cannot be decoded is recorded as an {@code undecodable} line, so its
 * offset still counts and the reconciler reports it.
 *
 * <p>Lines are buffered and reach the file on {@link #flush()}, which the subscriber thread calls after every poll.
 * Opening an existing file reads it back: the known identities, and {@link #resumeOffset()}, one past the highest
 * recorded channel offset. A restart therefore loses nothing; at most the tail received after the last flush is
 * received again, and recorded as redeliveries.
 */
public final class ReceiptWriter implements Closeable {
    private static final int BUFFER_CHARS = 1 << 16;

    private final String subscriberId;
    private final Path file;
    private final BufferedWriter writer;
    private final Set<String> known = new HashSet<>();
    private long highestOffset = -1;
    private long received;
    private long redeliveries;
    private long undecodable;

    public ReceiptWriter(Path receiptsDir, String subscriberId) throws IOException {
        this.subscriberId = subscriberId;
        Files.createDirectories(receiptsDir);
        this.file = receiptsDir.resolve(subscriberId + ".jsonl");
        if (Files.exists(file)) readBack();
        this.writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(file, StandardOpenOption.CREATE,
                StandardOpenOption.APPEND, StandardOpenOption.WRITE), StandardCharsets.UTF_8), BUFFER_CHARS);
    }

    private void readBack() throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        int complete = bytes.length;
        while (complete > 0 && bytes[complete - 1] != '\n') complete--;
        if (complete < bytes.length) {
            // A crash mid-write can tear only the last line; drop it, and its record is received again.
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
                channel.truncate(complete);
            }
        }
        List<String> lines = List.of(new String(bytes, 0, complete, StandardCharsets.UTF_8).split("\n"));
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) continue;
            Map<String, Object> receipt;
            try {
                receipt = Json.parseObject(line);
            } catch (RuntimeException corrupt) {
                throw new IOException("Corrupt receipt line " + (i + 1) + " in " + file, corrupt);
            }
            highestOffset = Math.max(highestOffset, Json.getLong(receipt, "channelOffset"));
            if (Json.getBoolean(receipt, "undecodable")) {
                undecodable++;
            } else if (known.add(Json.getString(receipt, "notificationId"))) {
                received++;
            } else {
                redeliveries++;
            }
        }
    }

    /** Records one received notification and returns whether it is a redelivery. */
    public boolean record(String notificationId, String saleId, String sku, int quantity, long completedAtMs,
                          long receivedAtMs, long channelOffset) throws IOException {
        boolean redelivery = !known.add(notificationId);
        if (redelivery) redeliveries++;
        else received++;
        highestOffset = Math.max(highestOffset, channelOffset);
        writer.write(Json.compact(Json.object(
                "subscriberId", subscriberId,
                "notificationId", notificationId,
                "saleId", saleId,
                "sku", sku,
                "quantity", (long) quantity,
                "completedAtMs", completedAtMs,
                "receivedAtMs", receivedAtMs,
                "channelOffset", channelOffset,
                "redelivery", redelivery)));
        writer.write('\n');
        return redelivery;
    }

    public void recordUndecodable(long channelOffset, long receivedAtMs, String error) throws IOException {
        undecodable++;
        highestOffset = Math.max(highestOffset, channelOffset);
        String detail = error == null ? "" : error;
        if (detail.length() > 300) detail = detail.substring(0, 300);
        writer.write(Json.compact(Json.object(
                "subscriberId", subscriberId,
                "undecodable", true,
                "channelOffset", channelOffset,
                "receivedAtMs", receivedAtMs,
                "error", detail)));
        writer.write('\n');
    }

    public void flush() throws IOException {
        writer.flush();
    }

    public long resumeOffset() { return highestOffset + 1; }

    public boolean knows(String notificationId) { return known.contains(notificationId); }

    /** Distinct notification identities recorded. */
    public long received() { return received; }

    public long redeliveries() { return redeliveries; }

    public long undecodable() { return undecodable; }

    public Path file() { return file; }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
