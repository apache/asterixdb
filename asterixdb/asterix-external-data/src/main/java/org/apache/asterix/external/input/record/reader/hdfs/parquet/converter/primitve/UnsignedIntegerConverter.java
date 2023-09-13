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
package org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.primitve;

import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.AbstractComplexConverter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.Warning;

public class UnsignedIntegerConverter extends GenericPrimitiveConverter {
    private boolean overflowed;

    UnsignedIntegerConverter(AbstractComplexConverter parent, String stringFieldName, int index,
            ParquetConverterContext context) throws IOException {
        super(ATypeTag.BIGINT, parent, stringFieldName, index, context);
        overflowed = false;
    }

    @Override
    public void addInt(int value) {
        addLong(value & 0x00000000ffffffffL);
    }

    @Override
    public void addLong(long value) {
        if (value < 0) {
            if (!overflowed) {
                Warning warning = Warning.of(null, ErrorCode.PARQUET_CONTAINS_OVERFLOWED_BIGINT, ATypeTag.BIGINT);
                context.getWarnings().add(warning);
                //Ensure this warning to be issued once
                overflowed = true;
            }
            return;
        }
        super.addLong(value);
    }
}
