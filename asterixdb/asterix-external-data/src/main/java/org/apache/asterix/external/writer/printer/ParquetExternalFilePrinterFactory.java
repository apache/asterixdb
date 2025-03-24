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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.writer.printer.parquet.SchemaConverterVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.writer.IExternalPrinter;
import org.apache.asterix.runtime.writer.IExternalPrinterFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

public class ParquetExternalFilePrinterFactory implements IExternalPrinterFactory {
    private static final long serialVersionUID = 8971234908711235L;
    // parquetInferredSchema is for the case when the schema is inferred from the data, not provided by the user
    // set During the runtime
    private transient MessageType parquetInferredSchema;
    // parquetProvidedSchema is for the case when the schema is provided by the user
    private ARecordType parquetProvidedSchema;
    private final IAType typeInfo;
    private final CompressionCodecName compressionCodecName;
    private final long rowGroupSize;
    private final int pageSize;
    private final ParquetProperties.WriterVersion writerVersion;

    public ParquetExternalFilePrinterFactory(CompressionCodecName compressionCodecName,
            ARecordType parquetprovidedSchema, IAType typeInfo, long rowGroupSize, int pageSize,
            ParquetProperties.WriterVersion writerVersion) {
        this.compressionCodecName = compressionCodecName;
        this.parquetProvidedSchema = parquetprovidedSchema;
        this.typeInfo = typeInfo;
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
        this.writerVersion = writerVersion;
    }

    public ParquetExternalFilePrinterFactory(CompressionCodecName compressionCodecName, IAType typeInfo,
            long rowGroupSize, int pageSize, ParquetProperties.WriterVersion writerVersion) {
        this.compressionCodecName = compressionCodecName;
        this.typeInfo = typeInfo;
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
        this.writerVersion = writerVersion;
    }

    public void setParquetSchema(MessageType parquetInferredSchema) {
        this.parquetInferredSchema = parquetInferredSchema;
    }

    @Override
    public IExternalPrinter createPrinter(IEvaluatorContext context) {
        if (parquetInferredSchema != null) {
            return new ParquetExternalFilePrinter(compressionCodecName, parquetInferredSchema, typeInfo, rowGroupSize,
                    pageSize, writerVersion);
        }

        MessageType schema;
        try {
            schema = SchemaConverterVisitor.convertToParquetSchema(parquetProvidedSchema);
        } catch (CompilationException e) {
            // This should not happen, Compilation Exception should be caught at the query-compile time
            throw new RuntimeException(e);
        }
        //TODO(ian): shouldn't this printer use the context to warn if there's a schema mismatch?
        return new ParquetExternalFilePrinter(compressionCodecName, schema, typeInfo, rowGroupSize, pageSize,
                writerVersion);
    }
}
