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

import static org.apache.asterix.common.utils.CSVConstants.KEY_NULL_STR;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.DelimitedDataParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class DelimitedDataParserFactory extends AbstractRecordStreamParserFactory<char[]> {

    private static final long serialVersionUID = 1L;
    private static final List<String> parserFormats =
            Collections.unmodifiableList(Arrays.asList(ExternalDataConstants.FORMAT_CSV,
                    ExternalDataConstants.FORMAT_DELIMITED_TEXT, ExternalDataConstants.FORMAT_TSV));

    @Override
    public IRecordDataParser<char[]> createRecordParser(IExternalDataRuntimeContext context)
            throws HyracksDataException {
        return createParser(context);
    }

    private DelimitedDataParser createParser(IExternalDataRuntimeContext context) throws HyracksDataException {
        IValueParserFactory[] valueParserFactories = ExternalDataUtils.getValueParserFactories(recordType);
        char delimiter = ExternalDataUtils.validateGetDelimiter(configuration);
        char quote = ExternalDataUtils.validateGetQuote(configuration, delimiter);
        char escape =
                ExternalDataUtils.validateGetEscape(configuration, configuration.get(ExternalDataConstants.KEY_FORMAT));
        boolean hasHeader = ExternalDataUtils.hasHeader(configuration);
        String nullString = configuration.get(KEY_NULL_STR);
        return new DelimitedDataParser(context, valueParserFactories, delimiter, quote, escape, hasHeader, recordType,
                ExternalDataUtils.getDataSourceType(configuration).equals(DataSourceType.STREAM), nullString);
    }

    @Override
    public Class<? extends char[]> getRecordClass() {
        return char[].class;
    }

    @Override
    public IStreamDataParser createInputStreamParser(IExternalDataRuntimeContext context) throws HyracksDataException {
        return createParser(context);
    }

    @Override
    public void setMetaType(ARecordType metaType) {
    }

    @Override
    public List<String> getParserFormats() {
        return parserFormats;
    }
}
