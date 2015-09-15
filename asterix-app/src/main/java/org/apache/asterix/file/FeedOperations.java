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
package org.apache.asterix.file;

import java.util.Collection;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedConnectJobInfo;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConstants;
import org.apache.asterix.common.feeds.FeedId;
import org.apache.asterix.common.feeds.FeedPolicyAccessor;
import org.apache.asterix.common.feeds.FeedTupleCommitResponseMessage;
import org.apache.asterix.common.feeds.api.IFeedJoint;
import org.apache.asterix.common.feeds.api.IFeedMessage;
import org.apache.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.common.feeds.message.EndFeedMessage;
import org.apache.asterix.common.feeds.message.ThrottlingEnabledFeedMessage;
import org.apache.asterix.feeds.FeedLifecycleListener;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.PrimaryFeed;
import org.apache.asterix.metadata.feeds.FeedMessageOperatorDescriptor;
import org.apache.asterix.metadata.feeds.IFeedAdapterFactory;
import org.apache.asterix.metadata.feeds.PrepareStallMessage;
import org.apache.asterix.metadata.feeds.TerminateDataFlowMessage;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
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
    public static Pair<JobSpecification, IFeedAdapterFactory> buildFeedIntakeJobSpec(PrimaryFeed primaryFeed,
            AqlMetadataProvider metadataProvider, FeedPolicyAccessor policyAccessor) throws Exception {

        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        spec.setFrameSize(FeedConstants.JobConstants.DEFAULT_FRAME_SIZE);
        IFeedAdapterFactory adapterFactory = null;
        IOperatorDescriptor feedIngestor;
        AlgebricksPartitionConstraint ingesterPc;

        try {
            Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IFeedAdapterFactory> t = metadataProvider
                    .buildFeedIntakeRuntime(spec, primaryFeed, policyAccessor);
            feedIngestor = t.first;
            ingesterPc = t.second;
            adapterFactory = t.third;
        } catch (AlgebricksException e) {
            e.printStackTrace();
            throw new AsterixException(e);
        }

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedIngestor, ingesterPc);

        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, ingesterPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedIngestor, 0, nullSink, 0);
        spec.addRoot(nullSink);
        return new Pair<JobSpecification, IFeedAdapterFactory>(spec, adapterFactory);
    }

    public static JobSpecification buildDiscontinueFeedSourceSpec(AqlMetadataProvider metadataProvider, FeedId feedId)
            throws AsterixException, AlgebricksException {

        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOperatorDescriptor feedMessenger = null;
        AlgebricksPartitionConstraint messengerPc = null;

        List<String> locations = FeedLifecycleListener.INSTANCE.getIntakeLocations(feedId);
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = buildDiscontinueFeedMessengerRuntime(spec, feedId,
                locations);

        feedMessenger = p.first;
        messengerPc = p.second;

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedMessenger, messengerPc);
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, messengerPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedMessenger, 0, nullSink, 0);
        spec.addRoot(nullSink);

        return spec;
    }

    /**
     * Builds the job spec for sending message to an active feed to disconnect it from the
     * its source.
     */
    public static Pair<JobSpecification, Boolean> buildDisconnectFeedJobSpec(AqlMetadataProvider metadataProvider,
            FeedConnectionId connectionId) throws AsterixException, AlgebricksException {

        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOperatorDescriptor feedMessenger;
        AlgebricksPartitionConstraint messengerPc;
        List<String> locations = null;
        FeedRuntimeType sourceRuntimeType;
        try {
            FeedConnectJobInfo cInfo = FeedLifecycleListener.INSTANCE.getFeedConnectJobInfo(connectionId);
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

    public static JobSpecification buildPrepareStallMessageJob(PrepareStallMessage stallMessage,
            Collection<String> collectLocations) throws AsterixException {
        JobSpecification messageJobSpec = JobSpecificationUtils.createJobSpecification();
        try {
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = FeedOperations.buildSendFeedMessageRuntime(
                    messageJobSpec, stallMessage.getConnectionId(), stallMessage, collectLocations);
            buildSendFeedMessageJobSpec(p.first, p.second, messageJobSpec);
        } catch (AlgebricksException ae) {
            throw new AsterixException(ae);
        }
        return messageJobSpec;
    }

    public static JobSpecification buildNotifyThrottlingEnabledMessageJob(
            ThrottlingEnabledFeedMessage throttlingEnabledMesg, Collection<String> locations) throws AsterixException {
        JobSpecification messageJobSpec = JobSpecificationUtils.createJobSpecification();
        try {
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = FeedOperations.buildSendFeedMessageRuntime(
                    messageJobSpec, throttlingEnabledMesg.getConnectionId(), throttlingEnabledMesg, locations);
            buildSendFeedMessageJobSpec(p.first, p.second, messageJobSpec);
        } catch (AlgebricksException ae) {
            throw new AsterixException(ae);
        }
        return messageJobSpec;
    }

    public static JobSpecification buildTerminateFlowMessageJob(TerminateDataFlowMessage terminateMessage,
            List<String> collectLocations) throws AsterixException {
        JobSpecification messageJobSpec = JobSpecificationUtils.createJobSpecification();
        try {
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = FeedOperations.buildSendFeedMessageRuntime(
                    messageJobSpec, terminateMessage.getConnectionId(), terminateMessage, collectLocations);
            buildSendFeedMessageJobSpec(p.first, p.second, messageJobSpec);
        } catch (AlgebricksException ae) {
            throw new AsterixException(ae);
        }
        return messageJobSpec;
    }

    public static JobSpecification buildCommitAckResponseJob(FeedTupleCommitResponseMessage commitResponseMessage,
            Collection<String> targetLocations) throws AsterixException {
        JobSpecification messageJobSpec = JobSpecificationUtils.createJobSpecification();
        try {
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = FeedOperations.buildSendFeedMessageRuntime(
                    messageJobSpec, commitResponseMessage.getConnectionId(), commitResponseMessage, targetLocations);
            buildSendFeedMessageJobSpec(p.first, p.second, messageJobSpec);
        } catch (AlgebricksException ae) {
            throw new AsterixException(ae);
        }
        return messageJobSpec;
    }

    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDiscontinueFeedMessengerRuntime(
            JobSpecification jobSpec, FeedId feedId, List<String> locations) throws AlgebricksException {
        FeedConnectionId feedConnectionId = new FeedConnectionId(feedId, null);
        IFeedMessage feedMessage = new EndFeedMessage(feedConnectionId, FeedRuntimeType.INTAKE,
                feedConnectionId.getFeedId(), true, EndFeedMessage.EndMessageType.DISCONTINUE_SOURCE);
        return buildSendFeedMessageRuntime(jobSpec, feedConnectionId, feedMessage, locations);
    }

    private static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildSendFeedMessageRuntime(
            JobSpecification jobSpec, FeedConnectionId feedConenctionId, IFeedMessage feedMessage,
            Collection<String> locations) throws AlgebricksException {
        AlgebricksPartitionConstraint partitionConstraint = new AlgebricksAbsolutePartitionConstraint(
                locations.toArray(new String[] {}));
        FeedMessageOperatorDescriptor feedMessenger = new FeedMessageOperatorDescriptor(jobSpec, feedConenctionId,
                feedMessage);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(feedMessenger, partitionConstraint);
    }

    private static JobSpecification buildSendFeedMessageJobSpec(IOperatorDescriptor operatorDescriptor,
            AlgebricksPartitionConstraint messengerPc, JobSpecification messageJobSpec) {
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(messageJobSpec, operatorDescriptor,
                messengerPc);
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(messageJobSpec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(messageJobSpec, nullSink, messengerPc);
        messageJobSpec.connect(new OneToOneConnectorDescriptor(messageJobSpec), operatorDescriptor, 0, nullSink, 0);
        messageJobSpec.addRoot(nullSink);
        return messageJobSpec;
    }

    private static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDisconnectFeedMessengerRuntime(
            JobSpecification jobSpec, FeedConnectionId feedConenctionId, List<String> locations,
            FeedRuntimeType sourceFeedRuntimeType, boolean completeDisconnection, FeedId sourceFeedId)
            throws AlgebricksException {
        IFeedMessage feedMessage = new EndFeedMessage(feedConenctionId, sourceFeedRuntimeType, sourceFeedId,
                completeDisconnection, EndFeedMessage.EndMessageType.DISCONNECT_FEED);
        return buildSendFeedMessageRuntime(jobSpec, feedConenctionId, feedMessage, locations);
    }
}
