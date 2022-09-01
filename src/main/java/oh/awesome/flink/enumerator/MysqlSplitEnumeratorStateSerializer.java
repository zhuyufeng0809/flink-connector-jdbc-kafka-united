package oh.awesome.flink.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MysqlSplitEnumeratorStateSerializer implements SimpleVersionedSerializer<MysqlSplitEnumeratorState> {

    private final Kryo kryo;

    public MysqlSplitEnumeratorStateSerializer() {
        this.kryo = new Kryo();
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(MysqlSplitEnumeratorState obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output output = new Output(bos);
        kryo.writeObject(output, obj);
        output.flush();
        return bos.toByteArray();
    }

    @Override
    public MysqlSplitEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        return kryo.readObject(new Input(serialized), MysqlSplitEnumeratorState.class);
    }
}
