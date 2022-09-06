package oh.awesome.flink.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@link RecordsWithSplitIds} instance can hold multi-split-data,
 * so that's why need implement {@link RecordsWithSplitIds#nextSplit()} method.
 */
public class MysqlRecordsWithSplitIds implements RecordsWithSplitIds<RowData> {
    private final String splitId;
    private final ResultSet resultSet;
    private Boolean firstCall;
    private Boolean finished;

    public MysqlRecordsWithSplitIds(String splitId, ResultSet resultSet) {
        this.splitId = splitId;
        this.resultSet = resultSet;
        this.firstCall = true;
        this.finished = false;
    }

    /**
     * This method is similar to {@link ResultSet#next()},
     * flink calls this method to change the cursor and determine if there are any remaining splits,
     * so if the {@link RecordsWithSplitIds} instance only holds one split inside,
     * the method will still be called twice,
     * the first call will point the cursor to the only split inside,
     * and the second call will return null to indicate that there are no splits left
     */
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
        try {
            if (resultSet.next()) {
                // todo
                return new GenericRowData(RowKind.INSERT, 1);
            } else {
                resultSet.close();
                finished = true;
                return null;
            }
        } catch (SQLException throwable) {
            throwable.printStackTrace();
            return null;
        }
    }

    @Override
    public Set<String> finishedSplits() {
        Set<String> finishedSplits = new HashSet<>();
        if (finished) {
            finishedSplits.add(splitId);
        }
        return finishedSplits;
    }
}
