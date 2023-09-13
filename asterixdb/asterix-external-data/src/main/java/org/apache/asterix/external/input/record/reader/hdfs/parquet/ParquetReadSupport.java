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
package org.apache.asterix.external.input.record.reader.hdfs.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.RootConverter;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class ParquetReadSupport extends ReadSupport<IValueReference> {
    private IExternalFilterValueEmbedder valueEmbedder;

    @Override
    public ReadContext init(InitContext context) {
        MessageType requestedSchema = getRequestedSchema(context);
        return new ReadContext(requestedSchema);
    }

    @Override
    public RecordMaterializer<IValueReference> prepareForRead(Configuration configuration,
            Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        try {
            return new ADMRecordMaterializer(configuration, valueEmbedder, readContext);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

    }

    private static MessageType getRequestedSchema(InitContext initContext) {
        Configuration configuration = initContext.getConfiguration();
        MessageType fileSchema = initContext.getFileSchema();

        List<Warning> warnings = new ArrayList<>();
        ParquetConverterContext context = new ParquetConverterContext(configuration, warnings);
        AsterixTypeToParquetTypeVisitor visitor = new AsterixTypeToParquetTypeVisitor(context);
        try {
            ARecordType expectedType = HDFSUtils.getExpectedType(configuration);
            Map<String, FunctionCallInformation> functionCallInformationMap =
                    HDFSUtils.getFunctionCallInformationMap(configuration);
            MessageType requestedType = visitor.clipType(expectedType, fileSchema, functionCallInformationMap);

            if (!warnings.isEmpty()) {
                //New warnings were created, set the warnings in hadoop configuration to be reported
                HDFSUtils.setWarnings(warnings, configuration);
                //Update the reported warnings so that we do not report the same warning again
                HDFSUtils.setFunctionCallInformationMap(functionCallInformationMap, configuration);
            }
            return requestedType;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    void setValueEmbedder(IExternalFilterValueEmbedder valueEmbedder) {
        this.valueEmbedder = valueEmbedder;
    }

    private static class ADMRecordMaterializer extends RecordMaterializer<IValueReference> {
        private final RootConverter rootConverter;
        private final List<Warning> warnings;
        private final Configuration configuration;

        public ADMRecordMaterializer(Configuration configuration, IExternalFilterValueEmbedder valueEmbedder,
                ReadContext readContext) throws IOException {
            warnings = new ArrayList<>();
            rootConverter = new RootConverter(readContext.getRequestedSchema(), valueEmbedder, configuration, warnings);
            this.configuration = configuration;
        }

        @Override
        public IValueReference getCurrentRecord() {
            try {
                if (!warnings.isEmpty()) {
                    //Issue all pending warnings
                    HDFSUtils.setWarnings(warnings, configuration);
                    warnings.clear();
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return rootConverter.getRecord();
        }

        @Override
        public GroupConverter getRootConverter() {
            return rootConverter;
        }
    }
}
