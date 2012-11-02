package edu.uci.ics.hivesterix.runtime.factory.normalize;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class HiveIntegerDescNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {

        return new INormalizedKeyComputer() {
            private VInt vint = new VInt();

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                LazyUtils.readVInt(bytes, start, vint);
                if (vint.length != length)
                    throw new IllegalArgumentException("length mismatch in int comparator function actual: "
                            + vint.length + " expected " + length);
                long unsignedValue = (long) vint.value;
                return (int) ((long)0xffffffff - unsignedValue);
            }
        };
    }
}
