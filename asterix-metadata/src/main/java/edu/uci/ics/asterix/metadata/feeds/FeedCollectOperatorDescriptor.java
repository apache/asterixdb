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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.IngestionRuntime;
import edu.uci.ics.asterix.common.feeds.SubscribableFeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleListener.ConnectionLocation;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.api.IFeedSubscriptionManager;
import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
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
 * FeedCollectOperatorDescriptor is responsible for ingesting data from an external source. This
 * operator uses a user specified for a built-in adaptor for retrieving data from the external
 * data source.
 */
public class FeedCollectOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(FeedCollectOperatorDescriptor.class.getName());

    /** The type associated with the ADM data output from the feed adaptor */
    private final IAType outputType;

    /** unique identifier for a feed instance. */
    private final FeedConnectionId connectionId;

    /** Map representation of policy parameters */
    private final Map<String, String> feedPolicyProperties;

    /** The (singleton) instance of {@code IFeedIngestionManager} **/
    private IFeedSubscriptionManager subscriptionManager;

    /** The source feed from which the feed derives its data from. **/
    private final FeedId sourceFeedId;

    /** The subscription location at which the recipient feed receives tuples from the source feed **/
    private final ConnectionLocation subscriptionLocation;

    public FeedCollectOperatorDescriptor(JobSpecification spec, FeedConnectionId feedConnectionId, FeedId sourceFeedId,
            ARecordType atype, RecordDescriptor rDesc, Map<String, String> feedPolicyProperties,
            ConnectionLocation subscriptionLocation) {
        super(spec, 0, 1);
        recordDescriptors[0] = rDesc;
        this.outputType = atype;
        this.connectionId = feedConnectionId;
        this.feedPolicyProperties = feedPolicyProperties;
        this.sourceFeedId = sourceFeedId;
        this.subscriptionLocation = subscriptionLocation;
    }

    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.subscriptionManager = runtimeCtx.getFeedManager().getFeedSubscriptionManager();
        ISubscribableRuntime sourceRuntime = null;
        IOperatorNodePushable nodePushable = null;
        switch (subscriptionLocation) {
            case SOURCE_FEED_INTAKE_STAGE:
                try {
                    SubscribableFeedRuntimeId feedSubscribableRuntimeId = new SubscribableFeedRuntimeId(sourceFeedId,
                            FeedRuntimeType.INTAKE, partition);
                    sourceRuntime = getIntakeRuntime(feedSubscribableRuntimeId);
                    if (sourceRuntime == null) {
                        throw new HyracksDataException("Source intake task not found for source feed id "
                                + sourceFeedId);
                    }
                    nodePushable = new FeedCollectOperatorNodePushable(ctx, sourceFeedId, connectionId,
                            feedPolicyProperties, partition, nPartitions, sourceRuntime);

                } catch (Exception exception) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Initialization of the feed adaptor failed with exception " + exception);
                    }
                    throw new HyracksDataException("Initialization of the feed adapter failed", exception);
                }
                break;
            case SOURCE_FEED_COMPUTE_STAGE:
                SubscribableFeedRuntimeId feedSubscribableRuntimeId = new SubscribableFeedRuntimeId(sourceFeedId,
                        FeedRuntimeType.COMPUTE, partition);
                sourceRuntime = (ISubscribableRuntime) subscriptionManager
                        .getSubscribableRuntime(feedSubscribableRuntimeId);
                if (sourceRuntime == null) {
                    throw new HyracksDataException("Source compute task not found for source feed id " + sourceFeedId
                            + " " + FeedRuntimeType.COMPUTE + "[" + partition + "]");
                }
                nodePushable = new FeedCollectOperatorNodePushable(ctx, sourceFeedId, connectionId,
                        feedPolicyProperties, partition, nPartitions, sourceRuntime);
                break;
        }
        return nodePushable;
    }

    public FeedConnectionId getFeedConnectionId() {
        return connectionId;
    }

    public Map<String, String> getFeedPolicyProperties() {
        return feedPolicyProperties;
    }

    public IAType getOutputType() {
        return outputType;
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptors[0];
    }

    public FeedId getSourceFeedId() {
        return sourceFeedId;
    }

    private IngestionRuntime getIntakeRuntime(SubscribableFeedRuntimeId subscribableRuntimeId) {
        int waitCycleCount = 0;
        ISubscribableRuntime ingestionRuntime = subscriptionManager.getSubscribableRuntime(subscribableRuntimeId);
        while (ingestionRuntime == null && waitCycleCount < 10) {
            try {
                Thread.sleep(2000);
                waitCycleCount++;
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("waiting to obtain ingestion runtime for subscription " + subscribableRuntimeId);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
            ingestionRuntime = subscriptionManager.getSubscribableRuntime(subscribableRuntimeId);
        }
        return (IngestionRuntime) ingestionRuntime;
    }

    public ConnectionLocation getSubscriptionLocation() {
        return subscriptionLocation;
    }
}
