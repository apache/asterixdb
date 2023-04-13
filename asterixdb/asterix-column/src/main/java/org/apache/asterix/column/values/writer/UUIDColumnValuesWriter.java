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
package org.apache.asterix.column.values.writer;

import org.apache.asterix.column.bytes.encoder.ParquetPlainFixedLengthValuesWriter;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.UUIDColumnFilterWriter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

final class UUIDColumnValuesWriter extends StringColumnValuesWriter {

    public UUIDColumnValuesWriter(Mutable<IColumnWriteMultiPageOp> multiPageOpRef, int columnIndex, int level,
            boolean collection, boolean filtered) {
        // UUID is always written without encoding
        super(columnIndex, level, collection, filtered, false, new ParquetPlainFixedLengthValuesWriter(multiPageOpRef));
    }

    @Override
    protected AbstractColumnFilterWriter createFilter() {
        return new UUIDColumnFilterWriter();
    }

    @Override
    protected ATypeTag getTypeTag() {
        return ATypeTag.UUID;
    }
}
