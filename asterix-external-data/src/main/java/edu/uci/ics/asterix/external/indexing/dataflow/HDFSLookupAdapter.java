/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.indexing.dataflow;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.HDFSIndexingAdapterFactory;
import edu.uci.ics.asterix.external.adapter.factory.StreamBasedAdapterFactory;
import edu.uci.ics.asterix.external.indexing.input.RCFileLookupReader;
import edu.uci.ics.asterix.external.indexing.input.SequenceFileLookupInputStream;
import edu.uci.ics.asterix.external.indexing.input.SequenceFileLookupReader;
import edu.uci.ics.asterix.external.indexing.input.TextFileLookupInputStream;
import edu.uci.ics.asterix.external.indexing.input.TextFileLookupReader;
import edu.uci.ics.asterix.metadata.external.ExternalFileIndexAccessor;
import edu.uci.ics.asterix.metadata.external.IControlledAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataParser;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class HDFSLookupAdapter implements IControlledAdapter, Serializable {

    private static final long serialVersionUID = 1L;

    private RecordDescriptor inRecDesc;
    private boolean propagateInput;
    private int[] ridFields;
    private int[] propagatedFields;
    private IAType atype;
    private Map<String, String> configuration;
    private IHyracksTaskContext ctx;
    private IControlledTupleParser parser;
    private ExternalFileIndexAccessor fileIndexAccessor;
    private boolean retainNull;

    public HDFSLookupAdapter(IAType atype, RecordDescriptor inRecDesc, Map<String, String> adapterConfiguration,
            boolean propagateInput, int[] ridFields, int[] propagatedFields, IHyracksTaskContext ctx,
            ExternalFileIndexAccessor fileIndexAccessor, boolean retainNull) {
        this.configuration = adapterConfiguration;
        this.atype = atype;
        this.ctx = ctx;
        this.inRecDesc = inRecDesc;
        this.propagatedFields = propagatedFields;
        this.propagateInput = propagateInput;
        this.propagatedFields = propagatedFields;
        this.fileIndexAccessor = fileIndexAccessor;
        this.ridFields = ridFields;
        this.retainNull = retainNull;
    }

    /*
     * This function is not easy to read and could be refactored into a better structure but for now it works
     */
    @Override
    public void initialize(IHyracksTaskContext ctx, INullWriterFactory iNullWriterFactory) throws Exception {
        JobConf jobConf = HDFSAdapterFactory.configureJobConf(configuration);
        // Create the lookup reader and the controlled parser
        if (configuration.get(HDFSAdapterFactory.KEY_INPUT_FORMAT).equals(HDFSAdapterFactory.INPUT_FORMAT_RC)) {
            configureRCFile(jobConf, iNullWriterFactory);
        } else if (configuration.get(AsterixTupleParserFactory.KEY_FORMAT).equals(AsterixTupleParserFactory.FORMAT_ADM)) {
            // create an adm parser
            ADMDataParser dataParser = new ADMDataParser();
            if (configuration.get(HDFSAdapterFactory.KEY_INPUT_FORMAT).equals(HDFSAdapterFactory.INPUT_FORMAT_TEXT)) {
                // Text input format
                TextFileLookupInputStream in = new TextFileLookupInputStream(fileIndexAccessor, jobConf);
                parser = new AdmOrDelimitedControlledTupleParser(ctx, (ARecordType) atype, in, propagateInput,
                        inRecDesc, dataParser, propagatedFields, ridFields, retainNull, iNullWriterFactory);
            } else {
                // Sequence input format
                SequenceFileLookupInputStream in = new SequenceFileLookupInputStream(fileIndexAccessor, jobConf);
                parser = new AdmOrDelimitedControlledTupleParser(ctx, (ARecordType) atype, in, propagateInput,
                        inRecDesc, dataParser, propagatedFields, ridFields, retainNull, iNullWriterFactory);
            }
        } else if (configuration.get(AsterixTupleParserFactory.KEY_FORMAT).equals(AsterixTupleParserFactory.FORMAT_DELIMITED_TEXT)) {
            // create a delimited text parser
            char delimiter = AsterixTupleParserFactory.getDelimiter(configuration);
            char quote = AsterixTupleParserFactory.getQuote(configuration, delimiter);

            DelimitedDataParser dataParser = HDFSIndexingAdapterFactory.getDelimitedDataParser((ARecordType) atype,
                    delimiter, quote);
            if (configuration.get(HDFSAdapterFactory.KEY_INPUT_FORMAT).equals(HDFSAdapterFactory.INPUT_FORMAT_TEXT)) {
                // Text input format
                TextFileLookupInputStream in = new TextFileLookupInputStream(fileIndexAccessor, jobConf);
                parser = new AdmOrDelimitedControlledTupleParser(ctx, (ARecordType) atype, in, propagateInput,
                        inRecDesc, dataParser, propagatedFields, ridFields, retainNull, iNullWriterFactory);
            } else {
                // Sequence input format
                SequenceFileLookupInputStream in = new SequenceFileLookupInputStream(fileIndexAccessor, jobConf);
                parser = new AdmOrDelimitedControlledTupleParser(ctx, (ARecordType) atype, in, propagateInput,
                        inRecDesc, dataParser, propagatedFields, ridFields, retainNull, iNullWriterFactory);
            }
        } else {
            configureGenericSeqOrText(jobConf, iNullWriterFactory);
        }
    }

    private void configureGenericSeqOrText(JobConf jobConf, INullWriterFactory iNullWriterFactory) throws IOException {
        if (configuration.get(HDFSAdapterFactory.KEY_INPUT_FORMAT).equals(HDFSAdapterFactory.INPUT_FORMAT_TEXT)) {
            // Text input format
            TextFileLookupReader reader = new TextFileLookupReader(fileIndexAccessor, jobConf);
            parser = new SeqOrTxtControlledTupleParser(ctx, createRecordParser(jobConf), reader, propagateInput,
                    propagatedFields, inRecDesc, ridFields, retainNull, iNullWriterFactory);
        } else {
            // Sequence input format
            SequenceFileLookupReader reader = new SequenceFileLookupReader(fileIndexAccessor, jobConf);
            parser = new SeqOrTxtControlledTupleParser(ctx, createRecordParser(jobConf), reader, propagateInput,
                    propagatedFields, inRecDesc, ridFields, retainNull, iNullWriterFactory);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer, IFrameWriter writer) throws Exception {
        parser.parseNext(writer, buffer);
    }

    @Override
    public void close(IFrameWriter writer) throws Exception {
        parser.close(writer);
    }

    @Override
    public void fail() throws Exception {
        // Do nothing
    }

    private void configureRCFile(Configuration jobConf, INullWriterFactory iNullWriterFactory) throws IOException,
            Exception {
        // RCFileLookupReader
        RCFileLookupReader reader = new RCFileLookupReader(fileIndexAccessor,
                HDFSAdapterFactory.configureJobConf(configuration));
        parser = new RCFileControlledTupleParser(ctx, createRecordParser(jobConf), reader, propagateInput,
                propagatedFields, inRecDesc, ridFields, retainNull, iNullWriterFactory);
    }

    private IAsterixHDFSRecordParser createRecordParser(Configuration jobConf) throws HyracksDataException {
        // Create the record parser
        // binary data with a special parser --> create the parser
        IAsterixHDFSRecordParser objectParser;
        if (configuration.get(HDFSAdapterFactory.KEY_PARSER).equals(HDFSAdapterFactory.PARSER_HIVE)) {
            objectParser = new HiveObjectParser();
        } else {
            try {
                objectParser = (IAsterixHDFSRecordParser) Class.forName(
                        configuration.get(HDFSAdapterFactory.KEY_PARSER)).newInstance();
            } catch (Exception e) {
                throw new HyracksDataException("Unable to create object parser", e);
            }
        }
        // initialize the parser
        try {
            objectParser.initialize((ARecordType) atype, configuration, jobConf);
        } catch (Exception e) {
            throw new HyracksDataException("Unable to initialize object parser", e);
        }

        return objectParser;
    }
}
