package analytics;

import persistence.PopularWindowDao;

import java.sql.SQLException;
import java.util.List;

/** Persistence boundary used by the recorder and deterministic failure-recovery tests. */
public interface AnalyticsWindowStore {
    long readMaxWindowEnd() throws SQLException;

    void writeWindow(long windowStart, long windowEnd,
                     List<PopularWindowDao.PopularEntry> entries) throws SQLException;
}
