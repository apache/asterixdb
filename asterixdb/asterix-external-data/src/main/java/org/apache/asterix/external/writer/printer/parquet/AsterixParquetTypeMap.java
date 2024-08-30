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
package org.apache.asterix.external.writer.printer.parquet;

import java.util.Map;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

public class AsterixParquetTypeMap {

    public static final Map<ATypeTag, PrimitiveType.PrimitiveTypeName> PRIMITIVE_TYPE_NAME_MAP =
            Map.ofEntries(Map.entry(ATypeTag.BOOLEAN, PrimitiveType.PrimitiveTypeName.BOOLEAN),
                    Map.entry(ATypeTag.STRING, PrimitiveType.PrimitiveTypeName.BINARY),
                    Map.entry(ATypeTag.TINYINT, PrimitiveType.PrimitiveTypeName.INT32),
                    Map.entry(ATypeTag.SMALLINT, PrimitiveType.PrimitiveTypeName.INT32),
                    Map.entry(ATypeTag.INTEGER, PrimitiveType.PrimitiveTypeName.INT32),
                    Map.entry(ATypeTag.BIGINT, PrimitiveType.PrimitiveTypeName.INT64),
                    Map.entry(ATypeTag.FLOAT, PrimitiveType.PrimitiveTypeName.FLOAT),
                    Map.entry(ATypeTag.DOUBLE, PrimitiveType.PrimitiveTypeName.DOUBLE),
                    Map.entry(ATypeTag.DATE, PrimitiveType.PrimitiveTypeName.INT32),
                    Map.entry(ATypeTag.TIME, PrimitiveType.PrimitiveTypeName.INT32),
                    Map.entry(ATypeTag.DATETIME, PrimitiveType.PrimitiveTypeName.INT64));

    public static final Map<ATypeTag, LogicalTypeAnnotation> LOGICAL_TYPE_ANNOTATION_MAP =
            Map.ofEntries(Map.entry(ATypeTag.STRING, LogicalTypeAnnotation.stringType()),
                    Map.entry(ATypeTag.DATE, LogicalTypeAnnotation.dateType()),
                    Map.entry(ATypeTag.TIME,
                            LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)),
                    Map.entry(ATypeTag.DATETIME,
                            LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)));

}
