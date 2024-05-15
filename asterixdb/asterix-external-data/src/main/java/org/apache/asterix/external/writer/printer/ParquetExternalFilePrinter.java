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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.asterix.external.input.record.reader.hdfs.parquet.AsterixParquetRuntimeException;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.writer.printer.parquet.AsterixParquetWriter;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.writer.IExternalPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class ParquetExternalFilePrinter implements IExternalPrinter {
    private final IAType typeInfo;
    private final CompressionCodecName compressionCodecName;
    private MessageType schema;
    private ParquetOutputFile parquetOutputFile;
    private String parquetSchemaString;
    private ParquetWriter<IValueReference> writer;
    private final long rowGroupSize;
    private final int pageSize;
    private final ParquetProperties.WriterVersion writerVersion;

    public ParquetExternalFilePrinter(CompressionCodecName compressionCodecName, String parquetSchemaString,
            IAType typeInfo, long rowGroupSize, int pageSize, ParquetProperties.WriterVersion writerVersion) {
        this.compressionCodecName = compressionCodecName;
        this.parquetSchemaString = parquetSchemaString;
        this.typeInfo = typeInfo;
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
        this.writerVersion = writerVersion;
    }

    @Override
    public void open() throws HyracksDataException {
        schema = MessageTypeParser.parseMessageType(parquetSchemaString);
    }

    @Override
    public void newStream(OutputStream outputStream) throws HyracksDataException {
        if (parquetOutputFile != null) {
            close();
        }
        parquetOutputFile = new ParquetOutputFile(outputStream);
        Configuration conf = new Configuration();

        try {
            writer = AsterixParquetWriter.builder(parquetOutputFile).withCompressionCodec(compressionCodecName)
                    .withType(schema).withTypeInfo(typeInfo).withRowGroupSize(rowGroupSize).withPageSize(pageSize)
                    .withDictionaryPageSize(ExternalDataConstants.PARQUET_DICTIONARY_PAGE_SIZE)
                    .enableDictionaryEncoding().withValidation(false).withWriterVersion(writerVersion).withConf(conf)
                    .build();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }

    @Override
    public void print(IValueReference value) throws HyracksDataException {
        try {
            this.writer.write(value);
        } catch (AsterixParquetRuntimeException e) {
            throw e.getHyracksDataException();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (this.writer != null) {
            try {
                this.writer.close();
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }
}
