package oh.awesome.flink.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.split.MySqlSplitState;

public class MysqlRecordEmitter implements RecordEmitter<RowData, RowData, MySqlSplitState> {
    @Override
    public void emitRecord(RowData element, SourceOutput<RowData> output, MySqlSplitState splitState) throws Exception {
        output.collect(element);
    }
}
