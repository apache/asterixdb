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
package org.apache.asterix.external.indexing.dataflow;

import java.util.Map;

import org.apache.asterix.external.adapter.factory.HDFSAdapterFactory;
import org.apache.asterix.external.adapter.factory.HDFSIndexingAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.operators.file.ADMDataParser;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory;
import org.apache.asterix.runtime.operators.file.DelimitedDataParser;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.ITupleParser;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * This is the parser factory for parsers used to do indexing
 */
public class HDFSIndexingParserFactory implements ITupleParserFactory {

    private static final long serialVersionUID = 1L;
    // file input-format <text, seq, rc>
    private final String inputFormat;
    // content format <adm, delimited-text, binary>
    private final String format;
    // delimiter in case of delimited text
    private final char delimiter;
    // quote in case of delimited text
    private final char quote;
    // parser class name in case of binary format
    private final String parserClassName;
    // the expected data type
    private final ARecordType atype;
    // the hadoop job conf
    private transient JobConf jobConf;
    // adapter arguments
    private Map<String, String> arguments;

    public HDFSIndexingParserFactory(ARecordType atype, String inputFormat, String format, char delimiter, char quote,
            String parserClassName) {
        this.inputFormat = inputFormat;
        this.format = format;
        this.parserClassName = parserClassName;
        this.delimiter = delimiter;
        this.quote = quote;
        this.atype = atype;
    }

    @Override
    public ITupleParser createTupleParser(IHyracksTaskContext ctx) throws HyracksDataException {
        if (format == null) {
            throw new IllegalArgumentException("Unspecified data format");
        }
        if (inputFormat == null) {
            throw new IllegalArgumentException("Unspecified data format");
        }
        if (!inputFormat.equalsIgnoreCase(HDFSAdapterFactory.INPUT_FORMAT_RC)
                && !inputFormat.equalsIgnoreCase(HDFSAdapterFactory.INPUT_FORMAT_TEXT)
                && !inputFormat.equalsIgnoreCase(HDFSAdapterFactory.INPUT_FORMAT_SEQUENCE)) {
            throw new IllegalArgumentException("External Indexing not supportd for format " + inputFormat);
        }
        // Do some real work here
        /*
         * Choices are:
         * 1. TxtOrSeq (Object) indexing tuple parser
         * 2. RC indexing tuple parser
         * 3. textual data tuple parser
         */
        if (format.equalsIgnoreCase(AsterixTupleParserFactory.FORMAT_ADM)) {
            // choice 3 with adm data parser
            ADMDataParser dataParser = new ADMDataParser();
            return new AdmOrDelimitedIndexingTupleParser(ctx, atype, dataParser);
        } else if (format.equalsIgnoreCase(AsterixTupleParserFactory.FORMAT_DELIMITED_TEXT)) {
            // choice 3 with delimited data parser
            DelimitedDataParser dataParser = HDFSIndexingAdapterFactory.getDelimitedDataParser(atype, delimiter, quote);
            return new AdmOrDelimitedIndexingTupleParser(ctx, atype, dataParser);
        }

        // binary data with a special parser --> create the parser
        IAsterixHDFSRecordParser objectParser;
        if (parserClassName.equalsIgnoreCase(HDFSAdapterFactory.PARSER_HIVE)) {
            objectParser = new HiveObjectParser();
        } else {
            try {
                objectParser = (IAsterixHDFSRecordParser) Class.forName(parserClassName).newInstance();
            } catch (Exception e) {
                throw new HyracksDataException("Unable to create object parser", e);
            }
        }
        try {
            objectParser.initialize(atype, arguments, jobConf);
        } catch (Exception e) {
            throw new HyracksDataException("Unable to initialize object parser", e);
        }

        if (inputFormat.equalsIgnoreCase(HDFSAdapterFactory.INPUT_FORMAT_RC)) {
            // Case 2
            return new RCFileIndexingTupleParser(ctx, atype, objectParser);
        } else {
            // Case 1
            return new TextOrSeqIndexingTupleParser(ctx, atype, objectParser);
        }
    }

    public JobConf getJobConf() {
        return jobConf;
    }

    public void setJobConf(JobConf jobConf) {
        this.jobConf = jobConf;
    }

    public Map<String, String> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, String> arguments) {
        this.arguments = arguments;
    }

}
