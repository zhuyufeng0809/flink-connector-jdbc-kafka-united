package oh.awesome.flink.split;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MySqlSplitSerializer implements SimpleVersionedSerializer<MySqlSplit> {
    private final Kryo kryo;

    public MySqlSplitSerializer() {
        this.kryo = new Kryo();
        kryo.register(MySqlSplit.class, new InternalMySqlSplitKryoSerializer());
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(MySqlSplit obj) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Output output = new Output(stream);
        kryo.writeObject(output, obj);
        output.flush();
        return stream.toByteArray();
    }

    @Override
    public MySqlSplit deserialize(int version, byte[] serialized) throws IOException {
        return kryo.readObject(new Input(serialized), MySqlSplit.class);
    }

    public static class InternalMySqlSplitKryoSerializer extends Serializer<MySqlSplit> {

        @Override
        public void write(Kryo kryo, Output output, MySqlSplit object) {
            ColumnMeta columnMeta = object.getColumnMeta();
            Range range = object.getRange();

            output.writeInt(object.getId());
            output.writeLong(range.getLowerBound());
            output.writeLong(range.getUpperBound());
            output.writeString(columnMeta.getSchemaName());
            output.writeString(columnMeta.getTableName());
            output.writeString(columnMeta.getColumnName());
        }

        @Override
        public MySqlSplit read(Kryo kryo, Input input, Class<MySqlSplit> type) {
            Integer id = input.readInt();
            Long lowerBound = input.readLong();
            Long upperBound = input.readLong();
            String schema = input.readString();
            String table = input.readString();
            String column = input.readString();

            return new MySqlSplit(
                    new ColumnMeta(schema, table, column),
                    new Range(lowerBound, upperBound),
                    id);
        }
    }
}
