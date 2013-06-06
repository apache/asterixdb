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
import edu.uci.ics.hivesterix.serde.lazy.LazyUtils.VInt;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class HiveIntegerAscNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

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
                return (int) ((unsignedValue - ((long) Integer.MIN_VALUE)) & 0xffffffffL);
            }
        };
    }
}
