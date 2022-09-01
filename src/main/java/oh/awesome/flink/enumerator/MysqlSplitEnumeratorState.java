package oh.awesome.flink.enumerator;

import oh.awesome.flink.split.MySqlSplit;

import java.util.HashSet;
import java.util.Set;

public class MysqlSplitEnumeratorState {
    // Set is un-duplicated
    private final Set<MySqlSplit> internalSplits;

    public MysqlSplitEnumeratorState(Set<MySqlSplit> splits) {
        this.internalSplits = new HashSet<>(splits);
    }

    public Set<MySqlSplit> getAssignedSplits() {
        return internalSplits;
    }
}
