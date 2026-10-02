package analytics;

import database.PopularWindowStore;
import database.StoreFailure;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;

/** Persists ranked windows strictly in FIFO order and exits only after the end marker. */
public final class PersistenceFilter implements Runnable {
    private static final long[] RETRY_DELAYS_MILLIS = {50L, 200L};

    private final BlockingQueue<PipelineMessage.RankingMessage> input;
    private final PopularWindowStore windowStore;
    private final AnalyticsLog log;
    private final RetryWait retryWait;

    public PersistenceFilter(
            BlockingQueue<PipelineMessage.RankingMessage> input,
            PopularWindowStore windowStore) {
        this(input, windowStore, AnalyticsLog.stderr(), Thread::sleep);
    }

    public PersistenceFilter(
            BlockingQueue<PipelineMessage.RankingMessage> input,
            PopularWindowStore windowStore,
            AnalyticsLog log,
            RetryWait retryWait) {
        this.input = Objects.requireNonNull(input, "input must not be null");
        this.windowStore = Objects.requireNonNull(windowStore, "windowStore must not be null");
        this.log = Objects.requireNonNull(log, "log must not be null");
        this.retryWait = Objects.requireNonNull(retryWait, "retryWait must not be null");
    }

    @Override
    public void run() {
        try {
            while (true) {
                PipelineMessage.RankingMessage message = input.take();
                if (message instanceof PipelineMessage.RankingEnd) return;
                persist((PipelineMessage.RankedWindow) message);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void persist(PipelineMessage.RankedWindow rankedWindow) throws InterruptedException {
        PopularWindowStore.PopularWindowSnapshot snapshot =
                new PopularWindowStore.PopularWindowSnapshot(
                        rankedWindow.windowStart(), rankedWindow.windowEnd(), mapRanks(rankedWindow));
        int failures = 0;
        while (true) {
            try {
                windowStore.writeWindow(snapshot);
                return;
            } catch (StoreFailure failure) {
                long delay = failures < RETRY_DELAYS_MILLIS.length
                        ? RETRY_DELAYS_MILLIS[failures]
                        : 1_000L;
                failures++;
                log.persistenceRetry(failures, rankedWindow.windowStart(),
                        rankedWindow.windowEnd(), failure);
                retryWait.await(delay);
            }
        }
    }

    @FunctionalInterface
    public interface RetryWait {
        void await(long delayMillis) throws InterruptedException;
    }

    private static List<PopularWindowStore.SnapshotRank> mapRanks(
            PipelineMessage.RankedWindow rankedWindow) {
        return rankedWindow.ranks().stream()
                .map(rank -> new PopularWindowStore.SnapshotRank(
                        rank.rank(), rank.sku(), rank.scanCount()))
                .toList();
    }
}
