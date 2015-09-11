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
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class UTF8StringNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer() {
            @Override
            public int normalize(byte[] bytes, int start, int length) {
                int len = UTF8StringPointable.getUTFLength(bytes, start);
                int nk = 0;
                int offset = start + 2;
                for (int i = 0; i < 2; ++i) {
                    nk <<= 16;
                    if (i < len) {
                        nk += ((int) UTF8StringPointable.charAt(bytes, offset)) & 0xffff;
                        offset += UTF8StringPointable.charSize(bytes, offset);
                    }
                }
                return nk;
            }
        };
    }
}