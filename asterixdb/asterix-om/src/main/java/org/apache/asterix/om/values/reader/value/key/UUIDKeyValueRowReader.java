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

import org.apache.asterix.dataflow.data.nontagged.comparators.AUUIDPartialBinaryComparatorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.values.reader.value.AbstractRowValueReader;
import org.apache.hyracks.data.std.api.IValueReference;

public final class UUIDKeyValueRowReader extends AbstractFixedLengthRowKeyValueReader {
    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.UUID;
    }

    @Override
    protected int getValueLength() {
        return 16;
    }

    @Override
    public IValueReference getBytes() {
        return value;
    }

    @Override
    public int compareTo(AbstractRowValueReader o) {
        return AUUIDPartialBinaryComparatorFactory.compare(getBytes(), o.getBytes());
    }
}
