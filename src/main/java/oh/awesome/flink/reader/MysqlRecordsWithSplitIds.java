package oh.awesome.flink.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * The {@link RecordsWithSplitIds} instance can hold multi-split-data,
 * so that's why need implement {@link RecordsWithSplitIds#nextSplit()} method.
 */
public class MysqlRecordsWithSplitIds implements RecordsWithSplitIds<RowData> {
    private final String splitId;
    private final Set<String> finishedSnapshotSplits;
    private ResultSet resultSet;
    private Boolean firstCall;

    public MysqlRecordsWithSplitIds(String splitId, Set<String> finishedSnapshotSplits) {
        this.splitId = splitId;
        this.finishedSnapshotSplits = finishedSnapshotSplits;
        this.firstCall = true;
    }

    //
    @Nullable
    @Override
    public String nextSplit() {
        if (firstCall) {
            firstCall = false;
            return splitId;
        } else {
            return null;
        }
    }

    @Nullable
    @Override
    public RowData nextRecordFromSplit() {
        return null;
    }

    @Override
    public Set<String> finishedSplits() {
        try {
            resultSet.close();
        } catch (SQLException throwable) {
            throwable.printStackTrace();
        }
        return finishedSnapshotSplits;
    }
}
