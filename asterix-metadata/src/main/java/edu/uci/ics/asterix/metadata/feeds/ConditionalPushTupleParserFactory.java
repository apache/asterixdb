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
package edu.uci.ics.asterix.metadata.feeds;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.asterix.runtime.operators.file.AbstractTupleParser;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataParser;
import edu.uci.ics.asterix.runtime.operators.file.IDataParser;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class ConditionalPushTupleParserFactory implements ITupleParserFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public ITupleParser createTupleParser(IHyracksTaskContext ctx) throws HyracksDataException {
        IDataParser dataParser = null;
        switch (parserType) {
            case ADM:
                dataParser = new ADMDataParser();
                break;
            case DELIMITED_DATA:
                dataParser = new DelimitedDataParser(recordType, valueParserFactories, delimiter, quote, false, -1, null);
                break;
        }
        return new ConditionalPushTupleParser(ctx, recordType, dataParser, configuration);
    }

    private final ARecordType recordType;
    private final Map<String, String> configuration;
    private IValueParserFactory[] valueParserFactories;
    private char delimiter;
    private char quote;
    private final ParserType parserType;

    public enum ParserType {
        ADM,
        DELIMITED_DATA
    }

    public ConditionalPushTupleParserFactory(ARecordType recordType, IValueParserFactory[] valueParserFactories,
            char fieldDelimiter, char quote, Map<String, String> configuration) {
        this.recordType = recordType;
        this.valueParserFactories = valueParserFactories;
        this.delimiter = fieldDelimiter;
        this.quote = quote;
        this.configuration = configuration;
        this.parserType = ParserType.DELIMITED_DATA;

    }

    public ConditionalPushTupleParserFactory(ARecordType recordType, Map<String, String> configuration) {
        this.recordType = recordType;
        this.configuration = configuration;
        this.parserType = ParserType.ADM;
    }

}

class ConditionalPushTupleParser extends AbstractTupleParser {

    private final IDataParser dataParser;
    private int batchSize;
    private long batchInterval;
    private boolean continueIngestion = true;
    private int tuplesInFrame = 0;
    private TimeBasedFlushTask flushTask;
    private Timer timer = new Timer();
    private Object lock = new Object();
    private boolean activeTimer = false;

    public static final String BATCH_SIZE = "batch-size";
    public static final String BATCH_INTERVAL = "batch-interval";

    public ConditionalPushTupleParser(IHyracksTaskContext ctx, ARecordType recType, IDataParser dataParser,
            Map<String, String> configuration) throws HyracksDataException {
        super(ctx, recType, false, -1, null);
        this.dataParser = dataParser;
        String propValue = (String) configuration.get(BATCH_SIZE);
        batchSize = propValue != null ? Integer.parseInt(propValue) : Integer.MAX_VALUE;
        propValue = (String) configuration.get(BATCH_INTERVAL);
        batchInterval = propValue != null ? Long.parseLong(propValue) : -1;
        activeTimer = batchInterval > 0;
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
        flushTask = new TimeBasedFlushTask(writer, lock);
        appender.reset(frame, true);
        IDataParser parser = getDataParser();
        try {
            parser.initialize(in, recType, true);
            if (activeTimer) {
                timer.schedule(flushTask, 0, batchInterval);
            }
            while (continueIngestion) {
                tb.reset();
                if (!parser.parse(tb.getDataOutput())) {
                    break;
                }
                tb.addFieldEndOffset();
                addTuple(writer);
            }
            if (appender.getTupleCount() > 0) {
                if (activeTimer) {
                    synchronized (lock) {
                        FrameUtils.flushFrame(frame, writer);
                    }
                } else {
                    FrameUtils.flushFrame(frame, writer);
                }
            }
        } catch (AsterixException ae) {
            throw new HyracksDataException(ae);
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        } finally {
            if (activeTimer) {
                timer.cancel();
            }
        }
    }

    protected void addTuple(IFrameWriter writer) throws HyracksDataException {
        if (activeTimer) {
            synchronized (lock) {
                addTupleToFrame(writer);
            }
        } else {
            addTupleToFrame(writer);
        }
    }

    protected void addTupleToFrame(IFrameWriter writer) throws HyracksDataException {
        if (tuplesInFrame == batchSize || !appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException();
            }
            if (tuplesInFrame == batchSize) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Batch size exceeded! flushing frame " + "(" + tuplesInFrame + ")");
                }
            }
            tuplesInFrame = 0;
        }
        tuplesInFrame++;
    }

    private class TimeBasedFlushTask extends TimerTask {

        private IFrameWriter writer;
        private final Object lock;

        public TimeBasedFlushTask(IFrameWriter writer, Object lock) {
            this.writer = writer;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                if (tuplesInFrame > 0) {
                    synchronized (lock) {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("TTL expired flushing frame (" + tuplesInFrame + ")");
                        }
                        FrameUtils.flushFrame(frame, writer);
                        appender.reset(frame, true);
                        tuplesInFrame = 0;
                    }
                }
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }

    }

}