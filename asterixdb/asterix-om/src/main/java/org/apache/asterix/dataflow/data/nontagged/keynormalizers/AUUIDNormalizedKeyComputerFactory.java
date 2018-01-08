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

package org.apache.asterix.dataflow.data.nontagged.keynormalizers;

import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyProperties;
import org.apache.hyracks.dataflow.common.data.normalizers.Integer64NormalizedKeyComputerFactory;

public class AUUIDNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {
    private static final long serialVersionUID = 1L;

    public static final INormalizedKeyProperties PROPERTIES = new INormalizedKeyProperties() {
        private static final long serialVersionUID = 1L;

        @Override
        public int getNormalizedKeyLength() {
            return 4;
        }

        @Override
        public boolean isDecisive() {
            return true;
        }
    };

    private final INormalizedKeyComputerFactory int64NormalizerFactory;
    private final int int64NormalizedKeyLength;

    public AUUIDNormalizedKeyComputerFactory() {
        int64NormalizerFactory = new Integer64NormalizedKeyComputerFactory();
        int64NormalizedKeyLength = int64NormalizerFactory.getNormalizedKeyProperties().getNormalizedKeyLength();
    }

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        final INormalizedKeyComputer nkc = int64NormalizerFactory.createNormalizedKeyComputer();
        return new INormalizedKeyComputer() {

            @Override
            public void normalize(byte[] bytes, int start, int length, int[] normalizedKeys, int keyStart) {
                // normalize msb
                nkc.normalize(bytes, start, length, normalizedKeys, keyStart);
                // normalize lsb
                nkc.normalize(bytes, start + Long.BYTES, length - Long.BYTES, normalizedKeys,
                        keyStart + int64NormalizedKeyLength);
            }

            @Override
            public INormalizedKeyProperties getNormalizedKeyProperties() {
                return PROPERTIES;
            }
        };

    }

    @Override
    public INormalizedKeyProperties getNormalizedKeyProperties() {
        return PROPERTIES;
    }

}
