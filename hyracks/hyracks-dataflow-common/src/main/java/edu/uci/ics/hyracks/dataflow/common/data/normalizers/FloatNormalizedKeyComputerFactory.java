package edu.uci.ics.hyracks.dataflow.common.data.normalizers;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class FloatNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer() {

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                int prefix = IntegerSerializerDeserializer.getInt(bytes, start);
                if (prefix >= 0) {
                    return prefix ^ Integer.MIN_VALUE;
                } else {
                    return (int) ((long) 0xffffffff - (long) prefix);
                }
            }

        };
    }

}
