package oh.awesome.flink.split;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MySqlSplitSerializer implements SimpleVersionedSerializer<MySqlSplit> {
    private final Kryo kryo;

    public MySqlSplitSerializer() {
        this.kryo = new Kryo();
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
}
