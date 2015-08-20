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
package edu.uci.ics.hyracks.dataflow.common.data.normalizers;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

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