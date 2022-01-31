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

import org.apache.asterix.external.input.record.reader.hdfs.parquet.AsterixTypeToParquetTypeVisitor;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.AbstractComplexConverter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class PrimitiveConverterProvider {
    public static final PrimitiveType MISSING =
            Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("MISSING");

    private PrimitiveConverterProvider() {
    }

    public static PrimitiveConverter createPrimitiveConverter(PrimitiveType type, AbstractComplexConverter parent,
            int index, ParquetConverterContext context) {
        return createPrimitiveConverter(type, parent, null, index, context);
    }

    public static PrimitiveConverter createPrimitiveConverter(PrimitiveType type, AbstractComplexConverter parent,
            IValueReference fieldName, int index, ParquetConverterContext context) {

        if (type == MISSING) {
            return MissingConverter.INSTANCE;
        }

        ATypeTag mappedType = AsterixTypeToParquetTypeVisitor.mapType(type, context, null);
        switch (mappedType) {
            case BOOLEAN:
            case STRING:
                return new GenericPrimitiveConverter(parent, fieldName, index, context);
            case BIGINT:
                return getIntConverter(type, parent, fieldName, index, context);
            case DOUBLE:
                return getDoubleConverter(type, parent, fieldName, index, context);
            case BINARY:
                return new BinaryConverter(parent, fieldName, index, context);
            case UUID:
                return new UUIDConverter(parent, fieldName, index, context);
            case DATE:
                return new DateConverter(parent, fieldName, index, context);
            case TIME:
                return getTimeConverter(type, parent, fieldName, index, context);
            case DATETIME:
                return getTimeStampConverter(type, parent, fieldName, index, context);
            case ANY:
                return new JsonStringConverter(parent, fieldName, index, context);
            default:
                return MissingConverter.INSTANCE;
        }
    }

    private static PrimitiveConverter getIntConverter(PrimitiveType type, AbstractComplexConverter parent,
            IValueReference fieldName, int index, ParquetConverterContext context) {
        IntLogicalTypeAnnotation intType = (IntLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
        if (intType != null && !intType.isSigned()) {
            return new UnsignedIntegerConverter(parent, fieldName, index, context);
        }
        return new GenericPrimitiveConverter(parent, fieldName, index, context);
    }

    private static PrimitiveConverter getDoubleConverter(PrimitiveType type, AbstractComplexConverter parent,
            IValueReference fieldName, int index, ParquetConverterContext context) {
        LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
        if (logicalType instanceof DecimalLogicalTypeAnnotation) {
            DecimalLogicalTypeAnnotation decimalLogicalType = (DecimalLogicalTypeAnnotation) logicalType;
            return new DecimalConverter(parent, fieldName, index, context, decimalLogicalType.getPrecision(),
                    decimalLogicalType.getScale());

        }
        return new GenericPrimitiveConverter(parent, fieldName, index, context);
    }

    private static PrimitiveConverter getTimeConverter(PrimitiveType type, AbstractComplexConverter parent,
            IValueReference fieldName, int index, ParquetConverterContext context) {
        TimeLogicalTypeAnnotation timeLogicalType = (TimeLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
        return new TimeConverter(parent, fieldName, index, context, timeLogicalType.getUnit());
    }

    private static PrimitiveConverter getTimeStampConverter(PrimitiveType type, AbstractComplexConverter parent,
            IValueReference fieldName, int index, ParquetConverterContext context) {
        TimestampLogicalTypeAnnotation tsType = (TimestampLogicalTypeAnnotation) type.getLogicalTypeAnnotation();
        if (tsType != null) {
            int offset = tsType.isAdjustedToUTC() ? context.getTimeZoneOffset() : 0;
            return new TimestampConverter(parent, fieldName, index, context, tsType.getUnit(), offset);
        }
        //INT96: the converter will convert the value to millis
        return new TimestampConverter(parent, fieldName, index, context, TimeUnit.MILLIS, 0);
    }
}
