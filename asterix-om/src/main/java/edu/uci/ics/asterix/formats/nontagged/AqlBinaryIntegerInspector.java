package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspector;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class AqlBinaryIntegerInspector implements IBinaryIntegerInspector {
    public static final IBinaryIntegerInspectorFactory FACTORY = new IBinaryIntegerInspectorFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IBinaryIntegerInspector createBinaryIntegerInspector(IHyracksTaskContext ctx) {
            return new AqlBinaryIntegerInspector();
        }
    };

    private AqlBinaryIntegerInspector() {
    }

    @Override
    public int getIntegerValue(byte[] bytes, int offset, int length) {
        return IntegerSerializerDeserializer.getInt(bytes, offset + 1);
    }
}