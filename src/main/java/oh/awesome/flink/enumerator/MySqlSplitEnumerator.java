package oh.awesome.flink.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.Preconditions;

import oh.awesome.flink.split.ColumnMeta;
import oh.awesome.flink.split.MySqlSplit;
import oh.awesome.flink.split.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MySqlSplitEnumerator implements SplitEnumerator<MySqlSplit, MysqlSplitEnumeratorState> {
    private final SplitEnumeratorContext<MySqlSplit> context;
    private final String schema = "";
    private final String table = "";
    private final String column = "";

    public MySqlSplitEnumerator(SplitEnumeratorContext<MySqlSplit> context) {
        this.context = context;
    }

    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    }

    @Override
    public void addSplitsBack(List<MySqlSplit> splits, int subtaskId) {
        // todo
    }

    @Override
    public void addReader(int subtaskId) {
        // todo
    }

    @Override
    public MysqlSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        // todo
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    private Set<MySqlSplit> calculateAndDerivedSplits(long minValue, long maxValue, int splitsNum) {
        Preconditions.checkArgument(splitsNum > 0, "Split number must be positive");

        long maxElemCount = (maxValue - minValue) + 1;
        int actualSplitsNum = splitsNum > maxElemCount ? (int) maxElemCount : splitsNum;
        long splitSize = new Double(Math.ceil((double) maxElemCount / actualSplitsNum)).longValue();
        long bigBatchNum = maxElemCount - (splitSize - 1) * actualSplitsNum;

        long start = minValue;
        Set<MySqlSplit> splits = new HashSet<>();
        for (int i = 0; i < minValue; i++) {
            long end = start + splitSize - 1 - (i >= bigBatchNum ? 1 : 0);
            splits.add(new MySqlSplit(
                    new ColumnMeta(schema, table, column),
                    new Range(start, end))
            );
            start = end + 1;
        }

        return splits;
    }
}
