package edu.uci.ics.pregelix.runtime.touchpoint;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.pregelix.api.util.SerDeUtils;

public class VLongAscNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer() {
            private static final int POSTIVE_LONG_MASK = (3 << 30);
            private static final int NON_NEGATIVE_INT_MASK = (2 << 30);
            private static final int NEGATIVE_LONG_MASK = (0 << 30);

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                long value = SerDeUtils.readVLong(bytes, start, length);
                int highValue = (int) (value >> 32);
                if (highValue > 0) {
                    /**
                     * larger than Integer.MAX
                     */
                    int highNmk = getKey(highValue);
                    highNmk >>= 2;
                    highNmk |= POSTIVE_LONG_MASK;
                    return highNmk;
                } else if (highValue == 0) {
                    /**
                     * smaller than Integer.MAX but >=0
                     */
                    int lowNmk = (int) value;
                    lowNmk >>= 2;
                    lowNmk |= NON_NEGATIVE_INT_MASK;
                    return lowNmk;
                } else {
                    /**
                     * less than 0; TODO: have not optimized for that
                     */
                    int highNmk = getKey(highValue);
                    highNmk >>= 2;
                    highNmk |= NEGATIVE_LONG_MASK;
                    return highNmk;
                }
            }

            private int getKey(int value) {
                long unsignedFirstValue = (long) value;
                int nmk = (int) ((unsignedFirstValue - ((long) Integer.MIN_VALUE)) & 0xffffffffL);
                return nmk;
            }

        };
    }
}
