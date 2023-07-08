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
package org.apache.asterix.om.values.reader.value.key;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.values.reader.value.AbstractRowValueReader;
import org.apache.hyracks.data.std.primitive.LongPointable;

public final class LongKeyRowValueReader extends AbstractFixedLengthRowKeyValueReader {
    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.BIGINT;
    }

    @Override
    protected int getValueLength() {
        return Long.BYTES;
    }

    @Override
    public long getLong() {
        return LongPointable.getLong(value.getByteArray(), value.getStartOffset());
    }

    @Override
    public int compareTo(AbstractRowValueReader o) {
        return Long.compare(getLong(), o.getLong());
    }
}
