package analytics;

import database.JdbcAcceptedScanReader;
import database.StoreFailure;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/** Reads committed scans into the bounded ingress pipe; the cursor is disposable. */
public final class AcceptedScanPoller implements Runnable {
    private static final int BATCH_SIZE = 256;
    private final JdbcAcceptedScanReader reader;
    private final BlockingQueue<PipelineMessage.IngressMessage> ingress;
    private long cursor;

    public AcceptedScanPoller(JdbcAcceptedScanReader reader,
            BlockingQueue<PipelineMessage.IngressMessage> ingress, long checkpoint) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.ingress = Objects.requireNonNull(ingress, "ingress");
        if (checkpoint < 0) throw new IllegalArgumentException("negative checkpoint");
        this.cursor = checkpoint;
    }

    @Override
    public void run() {
        int consecutiveReadFailures = 0;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                List<PipelineMessage.AcceptedScan> scans;
                try {
                    reader.materializeBatch(BATCH_SIZE);
                    scans = reader.readAfter(cursor, BATCH_SIZE);
                    consecutiveReadFailures = 0;
                } catch (StoreFailure unavailable) {
                    if (++consecutiveReadFailures == 1) {
                        System.err.println("[analytics] accepted-scan polling unavailable; retrying");
                    }
                    Thread.sleep(250);
                    continue;
                }
                if (scans.isEmpty()) {
                    Thread.sleep(100);
                    continue;
                }
                for (PipelineMessage.AcceptedScan scan : scans) {
                    ingress.put(scan);
                    cursor = scan.position();
                }
            }
        } catch (InterruptedException stopped) {
            Thread.currentThread().interrupt();
        }
    }
}
