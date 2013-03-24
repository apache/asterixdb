package edu.uci.ics.hivesterix.runtime.factory.normalize;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class HiveStringAscNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {

        return new INormalizedKeyComputer() {
            private VInt len = new VInt();

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                LazyUtils.readVInt(bytes, start, len);

                if (len.value + len.length != length)
                    throw new IllegalStateException("parse string: length mismatch, expected "
                            + (len.value + len.length) + " but get " + length);
                int nk = 0;
                int offset = start + len.length;
                for (int i = 0; i < 2; ++i) {
                    nk <<= 16;
                    if (i < len.value) {
                        char character = UTF8StringPointable.charAt(bytes, offset);
                        nk += ((int) character) & 0xffff;
                        offset += UTF8StringPointable.charSize(bytes, offset);
                    }
                }
                return nk;
            }
        };
    }
}
