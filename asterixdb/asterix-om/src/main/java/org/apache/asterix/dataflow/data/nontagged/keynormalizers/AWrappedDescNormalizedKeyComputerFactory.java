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

/**
 * This class uses a decorator pattern to wrap an ASC ordered INomralizedKeyComputerFactory implementation
 * to obtain the DESC order.
 */
public class AWrappedDescNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;
    private final INormalizedKeyComputerFactory nkcf;
    private final int normalizedKeyLength;

    public AWrappedDescNormalizedKeyComputerFactory(INormalizedKeyComputerFactory nkcf) {
        this.nkcf = nkcf;
        this.normalizedKeyLength = nkcf.getNormalizedKeyProperties().getNormalizedKeyLength();
    }

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        final INormalizedKeyComputer nkc = nkcf.createNormalizedKeyComputer();
        return new INormalizedKeyComputer() {

            @Override
            public void normalize(byte[] bytes, int start, int length, int[] normalizedKeys, int keyStart) {
                nkc.normalize(bytes, start + 1, length - 1, normalizedKeys, keyStart);
                for (int i = 0; i < normalizedKeyLength; i++) {
                    int key = normalizedKeys[keyStart + i];
                    normalizedKeys[keyStart + i] = ~key;
                }
            }

            @Override
            public INormalizedKeyProperties getNormalizedKeyProperties() {
                return nkc.getNormalizedKeyProperties();
            }
        };
    }

    @Override
    public INormalizedKeyProperties getNormalizedKeyProperties() {
        return nkcf.getNormalizedKeyProperties();
    }

}
