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
package org.apache.asterix.app.external;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.asterix.active.ActiveJobNotificationHandler;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveMessage;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.feed.api.IFeedJoint;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedEventsListener;
import org.apache.asterix.external.feed.message.EndFeedMessage;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedConnectJobInfo;
import org.apache.asterix.external.operators.FeedMessageOperatorDescriptor;
import org.apache.asterix.external.util.FeedConstants;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.file.JobSpecificationUtils;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileRemoveOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;

/**
 * Provides helper method(s) for creating JobSpec for operations on a feed.
 */
public class FeedOperations {

    /**
     * Builds the job spec for ingesting a (primary) feed from its external source via the feed adaptor.
     *
     * @param primaryFeed
     * @param metadataProvider
     * @return JobSpecification the Hyracks job specification for receiving data from external source
     * @throws Exception
     */
    public static Pair<JobSpecification, IAdapterFactory> buildFeedIntakeJobSpec(Feed primaryFeed,
            MetadataProvider metadataProvider, FeedPolicyAccessor policyAccessor) throws Exception {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        spec.setFrameSize(FeedConstants.JobConstants.DEFAULT_FRAME_SIZE);
        IAdapterFactory adapterFactory = null;
        IOperatorDescriptor feedIngestor;
        AlgebricksPartitionConstraint ingesterPc;
        Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IAdapterFactory> t =
                metadataProvider.buildFeedIntakeRuntime(spec, primaryFeed, policyAccessor);
        feedIngestor = t.first;
        ingesterPc = t.second;
        adapterFactory = t.third;
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedIngestor, ingesterPc);
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, ingesterPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedIngestor, 0, nullSink, 0);
        spec.addRoot(nullSink);
        return new Pair<JobSpecification, IAdapterFactory>(spec, adapterFactory);
    }

    /**
     * Builds the job spec for sending message to an active feed to disconnect it from the
     * its source.
     */
    public static Pair<JobSpecification, Boolean> buildDisconnectFeedJobSpec(MetadataProvider metadataProvider,
            FeedConnectionId connectionId) throws AsterixException, AlgebricksException {

        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOperatorDescriptor feedMessenger;
        AlgebricksPartitionConstraint messengerPc;
        List<String> locations = null;
        FeedRuntimeType sourceRuntimeType;
        try {
            FeedEventsListener listener = (FeedEventsListener) ActiveJobNotificationHandler.INSTANCE
                    .getActiveEntityListener(connectionId.getFeedId());
            FeedConnectJobInfo cInfo = listener.getFeedConnectJobInfo(connectionId);
            IFeedJoint sourceFeedJoint = cInfo.getSourceFeedJoint();
            IFeedJoint computeFeedJoint = cInfo.getComputeFeedJoint();

            boolean terminateIntakeJob = false;
            boolean completeDisconnect = computeFeedJoint == null || computeFeedJoint.getReceivers().isEmpty();
            if (completeDisconnect) {
                sourceRuntimeType = FeedRuntimeType.INTAKE;
                locations = cInfo.getCollectLocations();
                terminateIntakeJob = sourceFeedJoint.getReceivers().size() == 1;
            } else {
                locations = cInfo.getComputeLocations();
                sourceRuntimeType = FeedRuntimeType.COMPUTE;
            }

            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = buildDisconnectFeedMessengerRuntime(spec,
                    connectionId, locations, sourceRuntimeType, completeDisconnect, sourceFeedJoint.getOwnerFeedId());

            feedMessenger = p.first;
            messengerPc = p.second;

            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedMessenger, messengerPc);
            NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, messengerPc);
            spec.connect(new OneToOneConnectorDescriptor(spec), feedMessenger, 0, nullSink, 0);
            spec.addRoot(nullSink);
            return new Pair<JobSpecification, Boolean>(spec, terminateIntakeJob);

        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

    }

    private static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildSendFeedMessageRuntime(
            JobSpecification jobSpec, FeedConnectionId feedConenctionId, IActiveMessage feedMessage,
            Collection<String> locations) throws AlgebricksException {
        AlgebricksPartitionConstraint partitionConstraint =
                new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[] {}));
        FeedMessageOperatorDescriptor feedMessenger =
                new FeedMessageOperatorDescriptor(jobSpec, feedConenctionId, feedMessage);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(feedMessenger, partitionConstraint);
    }

    private static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDisconnectFeedMessengerRuntime(
            JobSpecification jobSpec, FeedConnectionId feedConenctionId, List<String> locations,
            FeedRuntimeType sourceFeedRuntimeType, boolean completeDisconnection, EntityId sourceFeedId)
            throws AlgebricksException {
        IActiveMessage feedMessage = new EndFeedMessage(feedConenctionId, sourceFeedRuntimeType, sourceFeedId,
                completeDisconnection, EndFeedMessage.EndMessageType.DISCONNECT_FEED);
        return buildSendFeedMessageRuntime(jobSpec, feedConenctionId, feedMessage, locations);
    }

    public static JobSpecification buildRemoveFeedStorageJob(Feed feed) throws Exception {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        AlgebricksAbsolutePartitionConstraint allCluster = ClusterStateManager.INSTANCE.getClusterLocations();
        Set<String> nodes = new TreeSet<>();
        for (String node : allCluster.getLocations()) {
            nodes.add(node);
        }
        AlgebricksAbsolutePartitionConstraint locations =
                new AlgebricksAbsolutePartitionConstraint(nodes.toArray(new String[nodes.size()]));
        FileSplit[] feedLogFileSplits =
                FeedUtils.splitsForAdapter(feed.getDataverseName(), feed.getFeedName(), locations);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint =
                StoragePathUtil.splitProviderAndPartitionConstraints(feedLogFileSplits);
        FileRemoveOperatorDescriptor frod = new FileRemoveOperatorDescriptor(spec, splitsAndConstraint.first, true);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, frod, splitsAndConstraint.second);
        spec.addRoot(frod);
        return spec;
    }
}
