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

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.DelimitedDataParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class DelimitedDataParserFactory extends AbstractRecordStreamParserFactory<char[]> {

    private static final long serialVersionUID = 1L;

    @Override
    public IRecordDataParser<char[]> createRecordParser(IHyracksTaskContext ctx)
            throws HyracksDataException, AsterixException {
        return createParser();
    }

    private DelimitedDataParser createParser() throws HyracksDataException, AsterixException {
        IValueParserFactory[] valueParserFactories = ExternalDataUtils.getValueParserFactories(recordType);
        Character delimiter = DelimitedDataParserFactory.getDelimiter(configuration);
        char quote = DelimitedDataParserFactory.getQuote(configuration, delimiter);
        boolean hasHeader = ExternalDataUtils.hasHeader(configuration);
        DelimitedDataParser parser = new DelimitedDataParser(valueParserFactories, delimiter, quote, hasHeader);
        parser.configure(configuration, recordType);
        return parser;
    }

    @Override
    public Class<? extends char[]> getRecordClass() {
        return char[].class;
    }

    @Override
    public IStreamDataParser createInputStreamParser(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException, AsterixException {
        return createParser();
    }

    // Get a delimiter from the given configuration
    public static char getDelimiter(Map<String, String> configuration) throws AsterixException {
        String delimiterValue = configuration.get(ExternalDataConstants.KEY_DELIMITER);
        if (delimiterValue == null) {
            delimiterValue = ExternalDataConstants.DEFAULT_DELIMITER;
        } else if (delimiterValue.length() != 1) {
            throw new AsterixException(
                    "'" + delimiterValue + "' is not a valid delimiter. The length of a delimiter should be 1.");
        }
        return delimiterValue.charAt(0);
    }

    // Get a quote from the given configuration when the delimiter is given
    // Need to pass delimiter to check whether they share the same character
    public static char getQuote(Map<String, String> configuration, char delimiter) throws AsterixException {
        String quoteValue = configuration.get(ExternalDataConstants.KEY_QUOTE);
        if (quoteValue == null) {
            quoteValue = ExternalDataConstants.DEFAULT_QUOTE;
        } else if (quoteValue.length() != 1) {
            throw new AsterixException("'" + quoteValue + "' is not a valid quote. The length of a quote should be 1.");
        }

        // Since delimiter (char type value) can't be null,
        // we only check whether delimiter and quote use the same character
        if (quoteValue.charAt(0) == delimiter) {
            throw new AsterixException(
                    "Quote '" + quoteValue + "' cannot be used with the delimiter '" + delimiter + "'. ");
        }

        return quoteValue.charAt(0);
    }
}
