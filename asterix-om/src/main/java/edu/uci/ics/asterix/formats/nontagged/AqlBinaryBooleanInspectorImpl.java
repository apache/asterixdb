package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryBooleanInspector;

public class AqlBinaryBooleanInspectorImpl implements IBinaryBooleanInspector {

    private static final long serialVersionUID = 1L;

    public static final AqlBinaryBooleanInspectorImpl INSTANCE = new AqlBinaryBooleanInspectorImpl();

    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    private AqlBinaryBooleanInspectorImpl() {
    }

    @Override
    public boolean getBooleanValue(byte[] bytes, int offset, int length) {
        if (bytes[offset] == SER_NULL_TYPE_TAG)
            return false;
        return bytes[offset + 1] == 1;
    }

}
