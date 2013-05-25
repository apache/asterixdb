package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class AqlBinaryBooleanInspectorImpl implements IBinaryBooleanInspector {
    public static final IBinaryBooleanInspectorFactory FACTORY = new IBinaryBooleanInspectorFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IBinaryBooleanInspector createBinaryBooleanInspector(IHyracksTaskContext ctx) {
            return new AqlBinaryBooleanInspectorImpl();
        }
    };

    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    private AqlBinaryBooleanInspectorImpl() {
    }

    @Override
    public boolean getBooleanValue(byte[] bytes, int offset, int length) {
        if (bytes[offset] == SER_NULL_TYPE_TAG)
            return false;
        /** check if the runtime type is boolean */
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
        if (typeTag != ATypeTag.BOOLEAN) {
            throw new IllegalStateException("Runtime error: the select condition should be of the boolean type!");
        }
        return bytes[offset + 1] == 1;
    }

}
