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
package org.apache.asterix.column.values.writer.filters;

import static org.apache.hyracks.util.string.UTF8StringUtil.charAt;
import static org.apache.hyracks.util.string.UTF8StringUtil.charSize;
import static org.apache.hyracks.util.string.UTF8StringUtil.getNumBytesToStoreLength;
import static org.apache.hyracks.util.string.UTF8StringUtil.getUTFLength;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class StringColumnFilterWriter extends LongColumnFilterWriter {
    @Override
    public void addValue(IValueReference value) throws HyracksDataException {
        addLong(normalize(value));
    }

    /**
     * Normalizes the string in a {@link Long}
     *
     * @see org.apache.hyracks.util.string.UTF8StringUtil#normalize(byte[], int)
     */
    public static long normalize(IValueReference value) {
        byte[] bytes = value.getByteArray();
        int start = value.getStartOffset();

        long nk = 0;
        int offset = start + getNumBytesToStoreLength(getUTFLength(bytes, start));
        int end = start + value.getLength();
        for (int i = 0; i < 4; ++i) {
            nk <<= 16;
            if (offset < end) {
                nk += (charAt(bytes, offset)) & 0xffff;
                offset += charSize(bytes, offset);
            }
        }
        return nk >>> 1;
    }
}
