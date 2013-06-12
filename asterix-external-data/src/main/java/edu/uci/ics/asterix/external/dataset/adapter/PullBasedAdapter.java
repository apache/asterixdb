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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.asterix.external.dataset.adapter.IPullBasedFeedClient.InflowState;
import edu.uci.ics.asterix.metadata.feeds.AbstractFeedDatasourceAdapter;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

/**
 * Acts as an abstract class for all pull-based external data adapters.
 * Captures the common logic for obtaining bytes from an external source
 * and packing them into frames as tuples.
 */
public abstract class PullBasedAdapter extends AbstractFeedDatasourceAdapter implements IDatasourceAdapter,
        IFeedAdapter {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PullBasedAdapter.class.getName());

    protected ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
    protected IPullBasedFeedClient pullBasedFeedClient;
    protected ARecordType adapterOutputType;
    private FrameTupleAppender appender;
    private ByteBuffer frame;
    protected boolean continueIngestion = true;
    protected boolean alterRequested = false;
    private Map<String, Object> modifiedConfiguration = null;
    private long tupleCount = 0;
    private final IHyracksTaskContext ctx;
    protected Map<String, Object> configuration;

    public abstract IPullBasedFeedClient getFeedClient(int partition) throws Exception;

    public PullBasedAdapter(Map<String, Object> configuration, IHyracksTaskContext ctx) {
        this.ctx = ctx;
        this.configuration = configuration;
    }

    public long getIngestedRecordsCount() {
        return tupleCount;
    }

    public void alter(Map<String, Object> modifedConfiguration) {
        this.modifiedConfiguration = modifedConfiguration;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        appender = new FrameTupleAppender(ctx.getFrameSize());
        frame = ctx.allocateFrame();
        appender.reset(frame, true);

        pullBasedFeedClient = getFeedClient(partition);
        InflowState inflowState = null;
        while (continueIngestion) {
            tupleBuilder.reset();
            try {
                inflowState = pullBasedFeedClient.nextTuple(tupleBuilder.getDataOutput());
                switch (inflowState) {
                    case DATA_AVAILABLE:
                        tupleBuilder.addFieldEndOffset();
                        appendTupleToFrame(writer);
                        tupleCount++;
                        break;
                    case NO_MORE_DATA:
                        LOGGER.info("Reached end of feed");
                        FrameUtils.flushFrame(frame, writer);
                        continueIngestion = false;
                        break;
                    case DATA_NOT_AVAILABLE:
                        break;
                }
                if (alterRequested) {
                    boolean success = pullBasedFeedClient.alter(modifiedConfiguration);
                    if (success) {
                        configuration = modifiedConfiguration;
                        modifiedConfiguration = null;
                    }
                }
            } catch (Exception failureException) {
                try {
                    boolean continueIngestion = policyEnforcer.handleSoftwareFailure(failureException);
                    if (continueIngestion) {
                        pullBasedFeedClient.resetOnFailure(failureException);
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
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
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
        dumpStatistics();
        timer.cancel();
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }

}
