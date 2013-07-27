/*
 * Copyright 2009-2012 by The Regents of the University of California
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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * Operator responsible for ingesting data from an external source. This
 * operator uses a (configurable) adapter associated with the feed dataset.
 */
public class FeedIntakeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(FeedIntakeOperatorDescriptor.class.getName());

    private final IAType atype;
    private final FeedConnectionId feedId;
    private final Map<String, String> feedPolicy;
    private IAdapterFactory adapterFactory;

    public FeedIntakeOperatorDescriptor(JobSpecification spec, FeedConnectionId feedId, IAdapterFactory adapterFactory,
            ARecordType atype, RecordDescriptor rDesc, Map<String, String> feedPolicy) {
        super(spec, 0, 1);
        recordDescriptors[0] = rDesc;
        this.adapterFactory = adapterFactory;
        this.atype = atype;
        this.feedId = feedId;
        this.feedPolicy = feedPolicy;
    }

    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        IFeedAdapter adapter;
        FeedRuntimeId feedRuntimeId = new FeedRuntimeId(FeedRuntimeType.INGESTION, feedId, partition);
        IngestionRuntime ingestionRuntime = (IngestionRuntime) FeedManager.INSTANCE.getFeedRuntime(feedRuntimeId);
        try {
            if (ingestionRuntime == null) {
                adapter = (IFeedAdapter) adapterFactory.createAdapter(ctx);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Beginning new feed:" + feedId);
                }
            } else {
                adapter = ((IngestionRuntime) ingestionRuntime).getAdapterRuntimeManager().getFeedAdapter();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Resuming old feed:" + feedId);
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException("initialization of adapter failed", e);
        }
        return new FeedIntakeOperatorNodePushable(ctx, feedId, adapter, feedPolicy, partition, ingestionRuntime);
    }

    public FeedConnectionId getFeedId() {
        return feedId;
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

    public IAdapterFactory getAdapterFactory() {
        return adapterFactory;
    }

    public IAType getAtype() {
        return atype;
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptors[0];
    }
}
