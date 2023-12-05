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

import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

public class ColumnValuesWriterFactory implements IColumnValuesWriterFactory {
    private final Mutable<IColumnWriteMultiPageOp> multiPageOpRef;

    public ColumnValuesWriterFactory(Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        this.multiPageOpRef = multiPageOpRef;
    }

    @Override
    public IColumnValuesWriter createValueWriter(ATypeTag typeTag, int columnIndex, int maxLevel, boolean writeAlways,
            boolean filtered) {
        switch (typeTag) {
            case MISSING:
            case NULL:
                return new NullMissingColumnValuesWriter(columnIndex, maxLevel, writeAlways, filtered);
            case BOOLEAN:
                return new BooleanColumnValuesWriter(columnIndex, maxLevel, writeAlways, filtered);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return new LongColumnValuesWriter(multiPageOpRef, columnIndex, maxLevel, writeAlways, filtered,
                        typeTag);
            case FLOAT:
                return new FloatColumnValuesWriter(multiPageOpRef, columnIndex, maxLevel, writeAlways, filtered);
            case DOUBLE:
                return new DoubleColumnValuesWriter(multiPageOpRef, columnIndex, maxLevel, writeAlways, filtered);
            case STRING:
                return new StringColumnValuesWriter(multiPageOpRef, columnIndex, maxLevel, writeAlways, filtered);
            case UUID:
                return new UUIDColumnValuesWriter(multiPageOpRef, columnIndex, maxLevel, writeAlways, filtered);
            default:
                throw new UnsupportedOperationException(typeTag + " is not supported");
        }
    }
}
