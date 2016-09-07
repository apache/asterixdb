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
package org.apache.asterix.external.provider;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.parser.factory.ADMDataParserFactory;
import org.apache.asterix.external.parser.factory.DelimitedDataParserFactory;
import org.apache.asterix.external.parser.factory.HiveDataParserFactory;
import org.apache.asterix.external.parser.factory.RSSParserFactory;
import org.apache.asterix.external.parser.factory.RecordWithMetadataParserFactory;
import org.apache.asterix.external.parser.factory.TweetParserFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;

public class ParserFactoryProvider {

    private ParserFactoryProvider() {
    }

    public static IDataParserFactory getDataParserFactory(ILibraryManager libraryManager,
            Map<String, String> configuration) throws AsterixException {
        IDataParserFactory parserFactory;
        String parserFactoryName = configuration.get(ExternalDataConstants.KEY_DATA_PARSER);
        if ((parserFactoryName != null) && ExternalDataUtils.isExternal(parserFactoryName)) {
            return ExternalDataUtils.createExternalParserFactory(libraryManager,
                    ExternalDataUtils.getDataverse(configuration), parserFactoryName);
        } else {
            String parserFactoryKey = ExternalDataUtils.getRecordFormat(configuration);
            if (parserFactoryKey == null) {
                parserFactoryKey = configuration.get(ExternalDataConstants.KEY_PARSER_FACTORY);
            }
            parserFactory = ParserFactoryProvider.getDataParserFactory(parserFactoryKey);
        }
        return parserFactory;
    }

    @SuppressWarnings("rawtypes")
    public static IDataParserFactory getDataParserFactory(String parser) throws AsterixException {
        switch (parser) {
            case ExternalDataConstants.FORMAT_ADM:
            case ExternalDataConstants.FORMAT_JSON:
            case ExternalDataConstants.FORMAT_SEMISTRUCTURED:
                return new ADMDataParserFactory();
            case ExternalDataConstants.FORMAT_DELIMITED_TEXT:
            case ExternalDataConstants.FORMAT_CSV:
                return new DelimitedDataParserFactory();
            case ExternalDataConstants.FORMAT_HIVE:
            case ExternalDataConstants.PARSER_HIVE:
                return new HiveDataParserFactory();
            case ExternalDataConstants.FORMAT_TWEET:
                return new TweetParserFactory();
            case ExternalDataConstants.FORMAT_RSS:
                return new RSSParserFactory();
            case ExternalDataConstants.FORMAT_RECORD_WITH_METADATA:
                return new RecordWithMetadataParserFactory();
            default:
                try {
                    return (IDataParserFactory) Class.forName(parser).newInstance();
                } catch (IllegalAccessException | ClassNotFoundException | InstantiationException
                        | ClassCastException e) {
                    throw new AsterixException("Unknown format: " + parser, e);
                }
        }
    }
}
