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
package org.apache.asterix.runtime.operators.file;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedPolicyAccessor;
import org.apache.asterix.common.parse.ITupleForwardPolicy;
import org.apache.asterix.common.parse.ITupleForwardPolicy.TupleForwardPolicyType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.LongParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.std.file.ITupleParser;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public class AsterixTupleParserFactory implements ITupleParserFactory {

    private static final long serialVersionUID = 1L;

    public static enum InputDataFormat {
        ADM,
        DELIMITED,
        UNKNOWN
    }

    public static final String HAS_HEADER = "has.header";
    public static final String KEY_FORMAT = "format";
    public static final String FORMAT_ADM = "adm";
    public static final String FORMAT_DELIMITED_TEXT = "delimited-text";
    public static final String FORMAT_BINARY = "binary";

    public static final String KEY_PATH = "path";
    public static final String KEY_SOURCE_DATATYPE = "type-name";
    public static final String KEY_DELIMITER = "delimiter";
    public static final String KEY_PARSER_FACTORY = "parser";
    public static final String KEY_HEADER = "header";
    public static final String KEY_QUOTE = "quote";
    public static final String TIME_TRACKING = "time.tracking";
    public static final String DEFAULT_QUOTE = "\"";
    public static final String AT_LEAST_ONE_SEMANTICS = FeedPolicyAccessor.AT_LEAST_ONE_SEMANTICS;
    public static final String NODE_RESOLVER_FACTORY_PROPERTY = "node.Resolver";
    public static final String DEFAULT_DELIMITER = ",";

    private static Map<ATypeTag, IValueParserFactory> valueParserFactoryMap = initializeValueParserFactoryMap();

    private static Map<ATypeTag, IValueParserFactory> initializeValueParserFactoryMap() {
        Map<ATypeTag, IValueParserFactory> m = new HashMap<ATypeTag, IValueParserFactory>();
        m.put(ATypeTag.INT32, IntegerParserFactory.INSTANCE);
        m.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        m.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        m.put(ATypeTag.INT64, LongParserFactory.INSTANCE);
        m.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
        return m;
    }

    private final ARecordType recordType;
    private final Map<String, String> configuration;
    private final InputDataFormat inputDataFormat;

    public AsterixTupleParserFactory(Map<String, String> configuration, ARecordType recType, InputDataFormat dataFormat) {
        this.recordType = recType;
        this.configuration = configuration;
        this.inputDataFormat = dataFormat;
    }

    @Override
    public ITupleParser createTupleParser(IHyracksTaskContext ctx) throws HyracksDataException {
        ITupleParser tupleParser = null;
        try {
            String parserFactoryClassname = (String) configuration.get(KEY_PARSER_FACTORY);
            ITupleParserFactory parserFactory = null;
            if (parserFactoryClassname != null) {
                parserFactory = (ITupleParserFactory) Class.forName(parserFactoryClassname).newInstance();
                tupleParser = parserFactory.createTupleParser(ctx);
            } else {
                IDataParser dataParser = null;
                dataParser = createDataParser(ctx);
                ITupleForwardPolicy policy = getTupleParserPolicy(configuration);
                policy.configure(configuration);
                tupleParser = new GenericTupleParser(ctx, recordType, dataParser, policy);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        return tupleParser;
    }

    private static class GenericTupleParser extends AbstractTupleParser {

        private final IDataParser dataParser;

        private final ITupleForwardPolicy policy;

        public GenericTupleParser(IHyracksTaskContext ctx, ARecordType recType, IDataParser dataParser,
                ITupleForwardPolicy policy) throws HyracksDataException {
            super(ctx, recType);
            this.dataParser = dataParser;
            this.policy = policy;
        }

        @Override
        public IDataParser getDataParser() {
            return dataParser;
        }

        @Override
        public ITupleForwardPolicy getTupleParserPolicy() {
            return policy;
        }

    }

    private IDataParser createDataParser(IHyracksTaskContext ctx) throws Exception {
        IDataParser dataParser = null;
        switch (inputDataFormat) {
            case ADM:
                dataParser = new ADMDataParser();
                break;
            case DELIMITED:
                dataParser = configureDelimitedDataParser(ctx);
                break;
            case UNKNOWN:
                String specifiedFormat = (String) configuration.get(KEY_FORMAT);
                if (specifiedFormat == null) {
                    throw new IllegalArgumentException(" Unspecified data format");
                } else {
                    if (FORMAT_ADM.equalsIgnoreCase(specifiedFormat.toUpperCase())) {
                        dataParser = new ADMDataParser();
                    } else if (FORMAT_DELIMITED_TEXT.equalsIgnoreCase(specifiedFormat.toUpperCase())) {
                        dataParser = configureDelimitedDataParser(ctx);
                    } else {
                        throw new IllegalArgumentException(" format " + configuration.get(KEY_FORMAT)
                                + " not supported");
                    }
                }
        }
        return dataParser;
    }

    public static ITupleForwardPolicy getTupleParserPolicy(Map<String, String> configuration) {
        ITupleForwardPolicy policy = null;
        ITupleForwardPolicy.TupleForwardPolicyType policyType = null;
        String propValue = configuration.get(ITupleForwardPolicy.PARSER_POLICY);
        if (propValue == null) {
            policyType = TupleForwardPolicyType.FRAME_FULL;
        } else {
            policyType = TupleForwardPolicyType.valueOf(propValue.trim().toUpperCase());
        }
        switch (policyType) {
            case FRAME_FULL:
                policy = new FrameFullTupleForwardPolicy();
                break;
            case COUNTER_TIMER_EXPIRED:
                policy = new CounterTimerTupleForwardPolicy();
                break;
            case RATE_CONTROLLED:
                policy = new RateControlledTupleForwardPolicy();
                break;
        }
        return policy;
    }

    private IDataParser configureDelimitedDataParser(IHyracksTaskContext ctx) throws AsterixException {
        IValueParserFactory[] valueParserFactories = getValueParserFactories();
        Character delimiter = getDelimiter(configuration);
        char quote = getQuote(configuration, delimiter);
        boolean hasHeader = hasHeader();
        return new DelimitedDataParser(recordType, valueParserFactories, delimiter, quote, hasHeader);
    }
  

    private boolean hasHeader() {
        String value = configuration.get(KEY_HEADER);
        if (value != null) {
            return Boolean.valueOf(value);
        }
        return false;
    }

    private IValueParserFactory[] getValueParserFactories() {
        int n = recordType.getFieldTypes().length;
        IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = null;
            if (recordType.getFieldTypes()[i].getTypeTag() == ATypeTag.UNION) {
                List<IAType> unionTypes = ((AUnionType) recordType.getFieldTypes()[i]).getUnionList();
                if (unionTypes.size() != 2 && unionTypes.get(0).getTypeTag() != ATypeTag.NULL) {
                    throw new NotImplementedException("Non-optional UNION type is not supported.");
                }
                tag = unionTypes.get(1).getTypeTag();
            } else {
                tag = recordType.getFieldTypes()[i].getTypeTag();
            }
            if (tag == null) {
                throw new NotImplementedException("Failed to get the type information for field " + i + ".");
            }
            IValueParserFactory vpf = valueParserFactoryMap.get(tag);
            if (vpf == null) {
                throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
            }
            fieldParserFactories[i] = vpf;
        }
        return fieldParserFactories;
    }

    // Get a delimiter from the given configuration
    public static char getDelimiter(Map<String, String> configuration) throws AsterixException {
        String delimiterValue = configuration.get(AsterixTupleParserFactory.KEY_DELIMITER);
        if (delimiterValue == null) {
            delimiterValue = AsterixTupleParserFactory.DEFAULT_DELIMITER;
        } else if (delimiterValue.length() != 1) {
            throw new AsterixException("'" + delimiterValue
                    + "' is not a valid delimiter. The length of a delimiter should be 1.");
        }
        return delimiterValue.charAt(0);
    }

    // Get a quote from the given configuration when the delimiter is given
    // Need to pass delimiter to check whether they share the same character
    public static char getQuote(Map<String, String> configuration, char delimiter) throws AsterixException {
        String quoteValue = configuration.get(AsterixTupleParserFactory.KEY_QUOTE);
        if (quoteValue == null) {
            quoteValue = AsterixTupleParserFactory.DEFAULT_QUOTE;
        } else if (quoteValue.length() != 1) {
            throw new AsterixException("'" + quoteValue + "' is not a valid quote. The length of a quote should be 1.");
        }

        // Since delimiter (char type value) can't be null,
        // we only check whether delimiter and quote use the same character
        if (quoteValue.charAt(0) == delimiter) {
            throw new AsterixException("Quote '" + quoteValue + "' cannot be used with the delimiter '" + delimiter
                    + "'. ");
        }

        return quoteValue.charAt(0);
    }

    // Get the header flag
    public static boolean getHasHeader(Map<String, String> configuration) {
        return Boolean.parseBoolean(configuration.get(AsterixTupleParserFactory.KEY_HEADER));
    }

}
