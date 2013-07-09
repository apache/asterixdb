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
package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.ITypedDatasourceAdapter;
import edu.uci.ics.asterix.feed.managed.adapter.IManagedFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.asterix.runtime.operators.file.AbstractTupleParser;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataParser;
import edu.uci.ics.asterix.runtime.operators.file.IDataParser;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * An adapter that simulates a feed from the contents of a source file. The file can be on the local file
 * system or on HDFS. The feed ends when the content of the source file has been ingested.
 */
public class RateControlledFileSystemBasedAdapter extends FileSystemBasedAdapter implements ITypedDatasourceAdapter,
        IManagedFeedAdapter {

    private static final long serialVersionUID = 1L;
    private FileSystemBasedAdapter coreAdapter;
    private String format;

    public RateControlledFileSystemBasedAdapter(ARecordType atype, Map<String, Object> configuration,
            FileSystemBasedAdapter coreAdapter, String format) throws Exception {
        super(atype);
        this.configuration = configuration;
        this.coreAdapter = coreAdapter;
        this.format = format;
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        return coreAdapter.getInputStream(partition);
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        coreAdapter.initialize(ctx);
        this.ctx = ctx;
    }

    @Override
    public void configure(Map<String, Object> arguments) throws Exception {
        coreAdapter.configure(arguments);
    }

    @Override
    public AdapterType getAdapterType() {
        return coreAdapter.getAdapterType();
    }

    @Override
    protected ITupleParser getTupleParser() throws Exception {
        ITupleParser parser = null;
        if (format.equals(FORMAT_DELIMITED_TEXT)) {
            parser = getRateControlledDelimitedDataTupleParser((ARecordType) atype);
        } else if (format.equals(FORMAT_ADM)) {
            parser = getRateControlledADMDataTupleParser((ARecordType) atype);
        } else {
            throw new IllegalArgumentException(" format " + configuration.get(KEY_FORMAT) + " not supported");
        }
        return parser;

    }

    protected ITupleParser getRateControlledDelimitedDataTupleParser(ARecordType recordType) throws AsterixException,
            HyracksDataException {
        ITupleParser parser;
        int n = recordType.getFieldTypes().length;
        IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = recordType.getFieldTypes()[i].getTypeTag();
            IValueParserFactory vpf = typeToValueParserFactMap.get(tag);
            if (vpf == null) {
                throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
            }
            fieldParserFactories[i] = vpf;

        }
        String delimiterValue = (String) configuration.get(KEY_DELIMITER);
        if (delimiterValue != null && delimiterValue.length() > 1) {
            throw new AsterixException("improper delimiter");
        }

        Character delimiter = delimiterValue.charAt(0);
        parser = new RateControlledTupleParserFactory(recordType, fieldParserFactories, delimiter, configuration)
                .createTupleParser(ctx);
        return parser;
    }

    protected ITupleParser getRateControlledADMDataTupleParser(ARecordType recordType) throws AsterixException {
        ITupleParser parser = null;
        try {
            parser = new RateControlledTupleParserFactory(recordType, configuration).createTupleParser(ctx);
            return parser;
        } catch (Exception e) {
            throw new AsterixException(e);
        }

    }

    @Override
    public ARecordType getAdapterOutputType() {
        return (ARecordType) atype;
    }

    @Override
    public void alter(Map<String, String> properties) {
        ((RateControlledTupleParser) parser).setInterTupleInterval(Long.parseLong(properties
                .get(RateControlledTupleParser.INTER_TUPLE_INTERVAL)));
    }

    @Override
    public void stop() {
        ((RateControlledTupleParser) parser).stop();
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return coreAdapter.getPartitionConstraint();
    }
}

class RateControlledTupleParserFactory implements ITupleParserFactory {

    private static final long serialVersionUID = 1L;

    private final ARecordType recordType;
    private final IDataParser dataParser;
    private final Map<String, Object> configuration;

    public RateControlledTupleParserFactory(ARecordType recordType, IValueParserFactory[] valueParserFactories,
            char fieldDelimiter, Map<String, Object> configuration) {
        this.recordType = recordType;
        dataParser = new DelimitedDataParser(recordType, valueParserFactories, fieldDelimiter);
        this.configuration = configuration;
    }

    public RateControlledTupleParserFactory(ARecordType recordType, Map<String, Object> configuration) {
        this.recordType = recordType;
        dataParser = new ADMDataParser();
        this.configuration = configuration;
    }

    @Override
    public ITupleParser createTupleParser(IHyracksTaskContext ctx) throws HyracksDataException {
        return new RateControlledTupleParser(ctx, recordType, dataParser, configuration);
    }

}

class RateControlledTupleParser extends AbstractTupleParser {

    private final IDataParser dataParser;
    private long interTupleInterval;
    private boolean delayConfigured;
    private boolean continueIngestion = true;

    public static final String INTER_TUPLE_INTERVAL = "tuple-interval";

    public RateControlledTupleParser(IHyracksTaskContext ctx, ARecordType recType, IDataParser dataParser,
            Map<String, Object> configuration) throws HyracksDataException {
        super(ctx, recType);
        this.dataParser = dataParser;
        String propValue = (String) configuration.get(INTER_TUPLE_INTERVAL);
        if (propValue != null) {
            interTupleInterval = Long.parseLong(propValue);
        } else {
            interTupleInterval = 0;
        }
        delayConfigured = interTupleInterval != 0;
    }

    public void setInterTupleInterval(long val) {
        this.interTupleInterval = val;
        this.delayConfigured = val > 0;
    }

    public void stop() {
        continueIngestion = false;
    }

    @Override
    public IDataParser getDataParser() {
        return dataParser;
    }

    @Override
    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {

        appender.reset(frame, true);
        IDataParser parser = getDataParser();
        try {
            parser.initialize(in, recType, true);
            while (continueIngestion) {
                tb.reset();
                if (!parser.parse(tb.getDataOutput())) {
                    break;
                }
                tb.addFieldEndOffset();
                if (delayConfigured) {
                    Thread.sleep(interTupleInterval);
                }
                addTupleToFrame(writer);
            }
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(frame, writer);
            }
        } catch (AsterixException ae) {
            throw new HyracksDataException(ae);
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        } catch (InterruptedException ie) {
            throw new HyracksDataException(ie);
        }
    }
}
