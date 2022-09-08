package oh.awesome.flink.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MysqlRecordsWithSplitIds implements RecordsWithSplitIds<RowData> {
    private final Map<String, Iterator<RowData>> splitRecords;
    private final Iterator<String> splitIdIterator;
    private final Set<String> finishedSnapshotSplits;
    private Iterator<RowData> currentSplitRecords;

    public MysqlRecordsWithSplitIds(Map<String, Iterator<RowData>> splitRecords,
                                    Set<String> finishedSnapshotSplits) {
        this.splitRecords = splitRecords;
        this.splitIdIterator = splitRecords.keySet().iterator();
        this.finishedSnapshotSplits = finishedSnapshotSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        if (splitIdIterator.hasNext()) {
            String splitId = splitIdIterator.next();
            currentSplitRecords = splitRecords.get(splitId);
            return splitId;
        } else {
            return null;
        }
    }

    @Nullable
    @Override
    public RowData nextRecordFromSplit() {
        if (currentSplitRecords.hasNext()) {
            return currentSplitRecords.next();
        } else {
            return null;
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSnapshotSplits;
    }
}
