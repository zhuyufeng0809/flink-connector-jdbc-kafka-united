package oh.awesome.flink.split;

public class MySqlSplitState {
    private final MySqlSplit split;

    public MySqlSplitState(MySqlSplit split) {
        this.split = split;
    }

    public MySqlSplit toMySqlSplit() {
        return split;
    }
}
