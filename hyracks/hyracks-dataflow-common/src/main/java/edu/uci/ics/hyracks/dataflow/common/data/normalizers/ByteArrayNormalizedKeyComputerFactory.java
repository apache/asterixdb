/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.dataflow.common.data.normalizers;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.data.std.primitive.ByteArrayPointable;

public class ByteArrayNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {
    public static ByteArrayNormalizedKeyComputerFactory INSTANCE = new ByteArrayNormalizedKeyComputerFactory();

    @Override public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer() {
            @Override public int normalize(byte[] bytes, int start, int length) {
                int normalizedKey = 0;
                int realLength = ByteArrayPointable.getLength(bytes, start);
                for (int i = 0; i < 3; ++i) {
                    normalizedKey <<= 8;
                    if (i < realLength) {
                        normalizedKey += bytes[start + ByteArrayPointable.SIZE_OF_LENGTH + i] & 0xff;
                    }
                }
                // last byte, shift 7 instead of 8 to avoid negative number
                normalizedKey <<= 7;
                if (3 < realLength) {
                    normalizedKey += (bytes[start + ByteArrayPointable.SIZE_OF_LENGTH + 3] & 0xfe) >> 1;
                }
                return normalizedKey;
            }
        };
    }
}
