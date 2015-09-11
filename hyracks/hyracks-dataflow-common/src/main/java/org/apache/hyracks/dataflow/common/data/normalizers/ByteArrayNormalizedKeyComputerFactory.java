/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.dataflow.common.data.normalizers;

import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;

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
