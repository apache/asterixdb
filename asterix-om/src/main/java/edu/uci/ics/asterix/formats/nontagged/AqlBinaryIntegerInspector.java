package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspector;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class AqlBinaryIntegerInspector implements IBinaryIntegerInspector {

    private static final long serialVersionUID = 1L;
    public static final AqlBinaryIntegerInspector INSTANCE = new AqlBinaryIntegerInspector();

    private AqlBinaryIntegerInspector() {
    }

    @Override
    public int getIntegerValue(byte[] bytes, int offset, int length) {
        return IntegerSerializerDeserializer.getInt(bytes, offset + 1);
    }

}
