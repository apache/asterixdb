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
package org.apache.asterix.external.writer.printer;

import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.writer.IExternalPrinter;
import org.apache.asterix.runtime.writer.IExternalPrinterFactory;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetExternalFilePrinterFactory implements IExternalPrinterFactory {
    private static final long serialVersionUID = 8971234908711234L;
    private final String parquetSchemaString;
    private final IAType typeInfo;
    private final CompressionCodecName compressionCodecName;
    private final long rowGroupSize;
    private final int pageSize;
    private final ParquetProperties.WriterVersion writerVersion;

    public ParquetExternalFilePrinterFactory(CompressionCodecName compressionCodecName, String parquetSchemaString,
            IAType typeInfo, long rowGroupSize, int pageSize, ParquetProperties.WriterVersion writerVersion) {
        this.compressionCodecName = compressionCodecName;
        this.parquetSchemaString = parquetSchemaString;
        this.typeInfo = typeInfo;
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
        this.writerVersion = writerVersion;
    }

    @Override
    public IExternalPrinter createPrinter() {
        return new ParquetExternalFilePrinter(compressionCodecName, parquetSchemaString, typeInfo, rowGroupSize,
                pageSize, writerVersion);
    }
}
