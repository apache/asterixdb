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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.input.record.RecordWithMetadataAndPK;
import org.apache.asterix.external.input.record.converter.IRecordConverterFactory;
import org.apache.asterix.external.parser.RecordWithMetadataParser;
import org.apache.asterix.external.provider.ParserFactoryProvider;
import org.apache.asterix.external.provider.RecordConverterFactoryProvider;
import org.apache.asterix.external.util.ExternalDataCompatibilityUtils;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class RecordWithMetadataParserFactory<I, O> implements IRecordDataParserFactory<I> {

    private static final long serialVersionUID = 1L;
    private static final List<String> parserFormats =
            Collections.unmodifiableList(Arrays.asList("record-with-metadata"));
    private ARecordType metaType;
    private ARecordType recordType;
    private IRecordDataParserFactory<O> recordParserFactory;
    private IRecordConverterFactory<I, RecordWithMetadataAndPK<O>> converterFactory;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, String> configuration) throws AlgebricksException {
        // validate first
        String recordFormat = configuration.get(ExternalDataConstants.KEY_RECORD_FORMAT);
        if (recordFormat == null) {
            throw AlgebricksException.create(ErrorCode.UNKNOWN_RECORD_FORMAT_FOR_META_PARSER,
                    ExternalDataConstants.KEY_FORMAT);
        }
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (format == null) {
            throw AlgebricksException.create(ErrorCode.UNKNOWN_RECORD_FORMAT_FOR_META_PARSER,
                    ExternalDataConstants.KEY_FORMAT);
        }
        // Create Parser Factory
        recordParserFactory = (IRecordDataParserFactory<O>) ParserFactoryProvider.getDataParserFactory(recordFormat);
        recordParserFactory.setRecordType(recordType);
        recordParserFactory.setMetaType(metaType);
        recordParserFactory.configure(configuration);
        // Create Converter Factory
        converterFactory = RecordConverterFactoryProvider.getConverterFactory(format, recordFormat);
        converterFactory.setRecordType(recordType);
        converterFactory.setMetaType(metaType);
        converterFactory.configure(configuration);
        // Validate Compatibility
        ExternalDataCompatibilityUtils.validateCompatibility(recordParserFactory, converterFactory);
    }

    @Override
    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public void setMetaType(ARecordType metaType) {
        this.metaType = metaType;
    }

    @Override
    public List<String> getParserFormats() {
        return parserFormats;
    }

    @Override
    public Class<?> getRecordClass() {
        return converterFactory.getInputClass();
    }

    @Override
    public IRecordDataParser<I> createRecordParser(IHyracksTaskContext ctx) throws HyracksDataException {
        IRecordDataParser<O> recordParser = recordParserFactory.createRecordParser(ctx);
        return new RecordWithMetadataParser<I, O>(metaType, recordParser, converterFactory.createConverter());
    }

}
