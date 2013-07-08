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

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public class HiveLongDescNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;
    private final INormalizedKeyComputerFactory ascNormalizedKeyComputerFactory = new HiveIntegerAscNormalizedKeyComputerFactory();

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer() {
            private INormalizedKeyComputer nmkComputer = ascNormalizedKeyComputerFactory.createNormalizedKeyComputer();

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                int nk = nmkComputer.normalize(bytes, start, length);
                return (int) ((long) Integer.MAX_VALUE - (long) (nk - Integer.MIN_VALUE));
            }

        };
    }

}
