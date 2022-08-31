package oh.awesome.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import oh.awesome.flink.enumerator.MysqlSplitEnumeratorState;
import oh.awesome.flink.split.MySqlSplit;

/**
 * only for snapshot reading in hybrid source
 */
public class MysqlSource implements Source<RowData, MySqlSplit, MysqlSplitEnumeratorState>, ResultTypeQueryable<RowData> {

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, MySqlSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<MySqlSplit, MysqlSplitEnumeratorState> createEnumerator(SplitEnumeratorContext<MySqlSplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<MySqlSplit, MysqlSplitEnumeratorState> restoreEnumerator(SplitEnumeratorContext<MySqlSplit> enumContext, MysqlSplitEnumeratorState checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<MySqlSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<MysqlSplitEnumeratorState> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }
}
