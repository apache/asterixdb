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
package org.apache.asterix.external.parser.factory;

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.input.record.RecordWithMetadata;
import org.apache.asterix.external.parser.RecordWithMetadataParser;
import org.apache.asterix.external.provider.ParserFactoryProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class RecordWithMetadataParserFactory<T> implements IRecordDataParserFactory<RecordWithMetadata<T>> {

    private static final long serialVersionUID = 1L;
    private Class<? extends RecordWithMetadata<T>> recordClass;
    private ARecordType recordType;
    private int[] metaIndexes;
    private IRecordDataParserFactory<T> valueParserFactory;
    private int valueIndex;

    @Override
    public DataSourceType getDataSourceType() throws AsterixException {
        return DataSourceType.RECORDS;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        // validation first
        if (!configuration.containsKey(ExternalDataConstants.KEY_META_INDEXES)) {
            throw new HyracksDataException(
                    "the parser parameter (" + ExternalDataConstants.KEY_META_INDEXES + ") is missing");
        }
        if (!configuration.containsKey(ExternalDataConstants.KEY_VALUE_INDEX)) {
            throw new HyracksDataException(
                    "the parser parameter (" + ExternalDataConstants.KEY_VALUE_INDEX + ") is missing");
        }
        if (!configuration.containsKey(ExternalDataConstants.KEY_VALUE_FORMAT)) {
            throw new HyracksDataException(
                    "the parser parameter (" + ExternalDataConstants.KEY_VALUE_FORMAT + ") is missing");
        }
        // get meta field indexes
        String[] stringMetaIndexes = configuration.get(ExternalDataConstants.KEY_META_INDEXES).split(",");
        metaIndexes = new int[stringMetaIndexes.length];
        for (int i = 0; i < stringMetaIndexes.length; i++) {
            metaIndexes[i] = Integer.parseInt(stringMetaIndexes[i].trim());
        }
        // get value index
        valueIndex = Integer.parseInt(configuration.get(ExternalDataConstants.KEY_VALUE_INDEX).trim());
        // get value format
        configuration.put(ExternalDataConstants.KEY_DATA_PARSER,
                configuration.get(ExternalDataConstants.KEY_VALUE_FORMAT));
        valueParserFactory = (IRecordDataParserFactory<T>) ParserFactoryProvider.getDataParserFactory(configuration);
        valueParserFactory.setRecordType((ARecordType) recordType.getFieldTypes()[valueIndex]);
        valueParserFactory.configure(configuration);
        recordClass = (Class<? extends RecordWithMetadata<T>>) (new RecordWithMetadata<T>(
                valueParserFactory.getRecordClass())).getClass();
    }

    @Override
    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public IRecordDataParser<RecordWithMetadata<T>> createRecordParser(IHyracksTaskContext ctx)
            throws HyracksDataException, AsterixException, IOException {
        IRecordDataParser<T> valueParser = valueParserFactory.createRecordParser(ctx);
        return new RecordWithMetadataParser<T>(recordClass, metaIndexes, valueParser, valueIndex);
    }

    @Override
    public Class<? extends RecordWithMetadata<T>> getRecordClass() {
        return recordClass;
    }
}
