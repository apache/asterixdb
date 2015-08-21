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
package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.external.dataset.adapter.IPullBasedFeedClient.InflowState;
import edu.uci.ics.asterix.metadata.feeds.FeedPolicyEnforcer;
import edu.uci.ics.asterix.metadata.feeds.IPullBasedFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

/**
 * Acts as an abstract class for all pull-based external data adapters. Captures
 * the common logic for obtaining bytes from an external source and packing them
 * into frames as tuples.
 */
public abstract class PullBasedAdapter implements IPullBasedFeedAdapter {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PullBasedAdapter.class.getName());
    private static final int timeout = 5; // seconds

    protected ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
    protected IPullBasedFeedClient pullBasedFeedClient;
    protected ARecordType adapterOutputType;
    protected boolean continueIngestion = true;
    protected Map<String, String> configuration;

    private FrameTupleAppender appender;
    private IFrame frame;
    private long tupleCount = 0;
    private final IHyracksTaskContext ctx;
    private int frameTupleCount = 0;

    protected FeedPolicyEnforcer policyEnforcer;

    public FeedPolicyEnforcer getPolicyEnforcer() {
        return policyEnforcer;
    }

    public void setFeedPolicyEnforcer(FeedPolicyEnforcer policyEnforcer) {
        this.policyEnforcer = policyEnforcer;
    }

    public abstract IPullBasedFeedClient getFeedClient(int partition) throws Exception;

    public PullBasedAdapter(Map<String, String> configuration, IHyracksTaskContext ctx) {
        this.ctx = ctx;
        this.configuration = configuration;
    }

    public long getIngestedRecordsCount() {
        return tupleCount;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        frame = new VSizeFrame(ctx);
        appender = new FrameTupleAppender(frame);

        pullBasedFeedClient = getFeedClient(partition);
        InflowState inflowState = null;

        while (continueIngestion) {
            tupleBuilder.reset();
            try {
                // blocking call
                inflowState = pullBasedFeedClient.nextTuple(tupleBuilder.getDataOutput(), timeout);
                switch (inflowState) {
                    case DATA_AVAILABLE:
                        tupleBuilder.addFieldEndOffset();
                        appendTupleToFrame(writer);
                        frameTupleCount++;
                        break;
                    case NO_MORE_DATA:
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Reached end of feed");
                        }
                        appender.flush(writer, true);
                        tupleCount += frameTupleCount;
                        frameTupleCount = 0;
                        continueIngestion = false;
                        break;
                    case DATA_NOT_AVAILABLE:
                        if (frameTupleCount > 0) {
                            appender.flush(writer, true);
                            tupleCount += frameTupleCount;
                            frameTupleCount = 0;
                        }
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Timed out on obtaining data from pull based adapter. Trying again!");
                        }
                        break;
                }

            } catch (Exception failureException) {
                try {
                    failureException.printStackTrace();
                    boolean continueIngestion = policyEnforcer.continueIngestionPostSoftwareFailure(failureException);
                    if (continueIngestion) {
                        tupleBuilder.reset();
                        continue;
                    } else {
                        throw failureException;
                    }
                } catch (Exception recoveryException) {
                    throw new Exception(recoveryException);
                }
            }
        }
    }

    private void appendTupleToFrame(IFrameWriter writer) throws HyracksDataException {
        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
            appender.flush(writer, true);
            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

    /**
     * Discontinue the ingestion of data and end the feed.
     * 
     * @throws Exception
     */
    public void stop() throws Exception {
        continueIngestion = false;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

}
