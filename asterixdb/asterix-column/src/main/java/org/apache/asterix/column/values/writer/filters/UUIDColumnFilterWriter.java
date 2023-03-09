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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.LongPointable;

/**
 * UUID filters are the LSB component of all written UUIDs. This could provide false positives UUIDs; however, this
 * still can filter out non-matching UUIDs.
 */
public class UUIDColumnFilterWriter extends LongColumnFilterWriter {

    @Override
    public void addValue(IValueReference value) throws HyracksDataException {
        addLong(getLSB(value));
    }

    public static long getLSB(IValueReference value) {
        byte[] bytes = value.getByteArray();
        int start = value.getStartOffset();
        return LongPointable.getLong(bytes, start + Long.BYTES);
    }
}
