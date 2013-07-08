/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hivesterix.runtime.factory.normalize;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VLong;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class HiveLongAscNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {

        return new INormalizedKeyComputer() {
            private static final int POSTIVE_LONG_MASK = (3 << 30);
            private static final int NON_NEGATIVE_INT_MASK = (2 << 30);
            private static final int NEGATIVE_LONG_MASK = (0 << 30);
            private VLong vlong = new VLong();

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                LazyUtils.readVLong(bytes, start, vlong);
                if (vlong.length != length)
                    throw new IllegalArgumentException("length mismatch in int comparator function actual: "
                            + vlong.length + " expected " + length);
                long value = (long) vlong.value;
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
