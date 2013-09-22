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

package edu.uci.ics.pregelix.core.runtime.touchpoint;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class RawNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {
    private static final long serialVersionUID = 1L;
    public static final INormalizedKeyComputerFactory INSTANCE = new RawNormalizedKeyComputerFactory();

    private RawNormalizedKeyComputerFactory() {

    }

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer() {

            @Override
            public int normalize(byte[] bytes, int start, int length) {
                int value = 0;
                if (length > 4) {
                    value = IntegerSerializerDeserializer.getInt(bytes, start);
                } else {
                    for (int i = start; i < start + length; i++) {
                        value = value << 8;
                        value += bytes[i] & 0xff;
                    }
                    for (int i = 0; i < 4 - length; i++) {
                        value = value << 8;
                    }
                }
                return value ^ Integer.MIN_VALUE;
            }

        };
    }
}
