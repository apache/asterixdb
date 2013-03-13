package edu.uci.ics.hivesterix.runtime.inspector;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspector;

public class HiveBinaryIntegerInspector implements IBinaryIntegerInspector {
    private VInt value = new VInt();

    HiveBinaryIntegerInspector() {
    }

    @Override
    public int getIntegerValue(byte[] bytes, int offset, int length) {
        LazyUtils.readVInt(bytes, offset, value);
        if (value.length != length)
            throw new IllegalArgumentException("length mismatch in int hash function actual: " + length + " expected "
                    + value.length);
        return value.value;
    }

}
