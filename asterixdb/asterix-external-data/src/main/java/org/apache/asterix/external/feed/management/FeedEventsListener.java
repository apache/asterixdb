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
package org.apache.asterix.external.feed.management;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActiveJob;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.external.feed.api.FeedOperationCounter;
import org.apache.asterix.external.feed.api.IFeedJoint;
import org.apache.asterix.external.feed.api.IFeedJoint.State;
import org.apache.asterix.external.feed.api.IFeedLifecycleEventSubscriber;
import org.apache.asterix.external.feed.api.IFeedLifecycleEventSubscriber.FeedLifecycleEvent;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedConnectJobInfo;
import org.apache.asterix.external.feed.watch.FeedIntakeInfo;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.external.util.FeedUtils.JobType;
import org.apache.asterix.runtime.util.AsterixAppContextInfo;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;
import org.apache.log4j.Logger;

public class FeedEventsListener implements IActiveEntityEventsListener {
    private static final Logger LOGGER = Logger.getLogger(FeedEventsListener.class);
    private final Map<EntityId, Pair<FeedOperationCounter, List<IFeedJoint>>> feedPipeline;
    private final List<IFeedLifecycleEventSubscriber> subscribers;
    private final Map<Long, ActiveJob> jobs;
    private final Map<Long, ActiveJob> intakeJobs;
    private final Map<EntityId, FeedIntakeInfo> entity2Intake;
    private final Map<FeedConnectionId, FeedConnectJobInfo> connectJobInfos;
    private EntityId entityId;
    private IFeedJoint sourceFeedJoint;

    public FeedEventsListener(EntityId entityId) {
        this.entityId = entityId;
        subscribers = new ArrayList<>();
        jobs = new HashMap<>();
        feedPipeline = new HashMap<>();
        entity2Intake = new HashMap<>();
        connectJobInfos = new HashMap<>();
        intakeJobs = new HashMap<>();
    }

    @Override
    public void notify(ActiveEvent event) {
        try {
            switch (event.getEventKind()) {
                case JOB_START:
                    handleJobStartEvent(event);
                    break;
                case JOB_FINISH:
                    handleJobFinishEvent(event);
                    break;
                case PARTITION_EVENT:
                    handlePartitionStart(event);
                    break;
                default:
                    LOGGER.warn("Unknown Feed Event" + event);
                    break;
            }
        } catch (Exception e) {
            LOGGER.error("Unhandled Exception", e);
        }
    }

    private synchronized void handleJobStartEvent(ActiveEvent message) throws Exception {
        ActiveJob jobInfo = jobs.get(message.getJobId().getId());
        JobType jobType = (JobType) jobInfo.getJobObject();
        switch (jobType) {
            case INTAKE:
                handleIntakeJobStartMessage((FeedIntakeInfo) jobInfo);
                break;
            case FEED_CONNECT:
                handleCollectJobStartMessage((FeedConnectJobInfo) jobInfo);
                break;
            default:
        }
    }

    private synchronized void handleJobFinishEvent(ActiveEvent message) throws Exception {
        ActiveJob jobInfo = jobs.get(message.getJobId().getId());
        JobType jobType = (JobType) jobInfo.getJobObject();
        switch (jobType) {
            case FEED_CONNECT:
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Collect Job finished for  " + jobInfo);
                }
                handleFeedCollectJobFinishMessage((FeedConnectJobInfo) jobInfo);
                break;
            case INTAKE:
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Intake Job finished for feed intake " + jobInfo.getJobId());
                }
                handleFeedIntakeJobFinishMessage((FeedIntakeInfo) jobInfo, message);
                break;
            default:
                break;
        }
    }

    private synchronized void handlePartitionStart(ActiveEvent message) {
        ActiveJob jobInfo = jobs.get(message.getJobId().getId());
        JobType jobType = (JobType) jobInfo.getJobObject();
        switch (jobType) {
            case FEED_CONNECT:
                ((FeedConnectJobInfo) jobInfo).partitionStart();
                if (((FeedConnectJobInfo) jobInfo).collectionStarted()) {
                    notifyFeedEventSubscribers(FeedLifecycleEvent.FEED_COLLECT_STARTED);
                }
                break;
            case INTAKE:
                handleIntakePartitionStarts(message, jobInfo);
                break;
            default:
                break;

        }
    }

    private void handleIntakePartitionStarts(ActiveEvent message, ActiveJob jobInfo) {
        if (feedPipeline.get(message.getEntityId()).first.decrementAndGet() == 0) {
            ((FeedIntakeInfo) jobInfo).getIntakeFeedJoint().setState(State.ACTIVE);
            jobInfo.setState(ActivityState.ACTIVE);
            notifyFeedEventSubscribers(FeedLifecycleEvent.FEED_INTAKE_STARTED);
        }
    }

    public synchronized void registerFeedJoint(IFeedJoint feedJoint, int numOfPrividers) {
        Pair<FeedOperationCounter, List<IFeedJoint>> feedJointsOnPipeline =
                feedPipeline.get(feedJoint.getOwnerFeedId());
        if (feedJointsOnPipeline == null) {
            feedJointsOnPipeline = new Pair<>(new FeedOperationCounter(numOfPrividers), new ArrayList<IFeedJoint>());
            feedPipeline.put(feedJoint.getOwnerFeedId(), feedJointsOnPipeline);
            feedJointsOnPipeline.second.add(feedJoint);
        } else {
            if (!feedJointsOnPipeline.second.contains(feedJoint)) {
                feedJointsOnPipeline.second.add(feedJoint);
            } else {
                throw new IllegalArgumentException("Feed joint " + feedJoint + " already registered");
            }
        }
    }

    public synchronized void deregisterFeedIntakeJob(JobId jobId) {
        FeedIntakeInfo info = (FeedIntakeInfo) intakeJobs.remove(jobId.getId());
        jobs.remove(jobId.getId());
        entity2Intake.remove(info.getFeedId());
        List<IFeedJoint> joints = feedPipeline.get(info.getFeedId()).second;
        joints.remove(info.getIntakeFeedJoint());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Deregistered feed intake job [" + jobId + "]");
        }
    }

    private static synchronized void handleIntakeJobStartMessage(FeedIntakeInfo intakeJobInfo) throws Exception {
        List<OperatorDescriptorId> intakeOperatorIds = new ArrayList<>();
        Map<OperatorDescriptorId, IOperatorDescriptor> operators = intakeJobInfo.getSpec().getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                intakeOperatorIds.add(opDesc.getOperatorId());
            }
        }

        IHyracksClientConnection hcc = AsterixAppContextInfo.INSTANCE.getHcc();
        JobInfo info = hcc.getJobInfo(intakeJobInfo.getJobId());
        List<String> intakeLocations = new ArrayList<>();
        for (OperatorDescriptorId intakeOperatorId : intakeOperatorIds) {
            Map<Integer, String> operatorLocations = info.getOperatorLocations().get(intakeOperatorId);
            int nOperatorInstances = operatorLocations.size();
            for (int i = 0; i < nOperatorInstances; i++) {
                intakeLocations.add(operatorLocations.get(i));
            }
        }
        // intakeLocations is an ordered list; 
        // element at position i corresponds to location of i'th instance of operator
        intakeJobInfo.setIntakeLocation(intakeLocations);
    }

    public IFeedJoint getSourceFeedJoint(FeedConnectionId connectionId) {
        FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
        if (cInfo != null) {
            return cInfo.getSourceFeedJoint();
        }
        return null;
    }

    public synchronized void registerFeedIntakeJob(EntityId feedId, JobId jobId, JobSpecification jobSpec)
            throws HyracksDataException {
        if (entity2Intake.get(feedId) != null) {
            throw new IllegalStateException("Feed already has an intake job");
        }
        if (intakeJobs.get(jobId.getId()) != null) {
            throw new IllegalStateException("Feed job already registered in intake jobs");
        }
        if (jobs.get(jobId.getId()) != null) {
            throw new IllegalStateException("Feed job already registered in all jobs");
        }

        Pair<FeedOperationCounter, List<IFeedJoint>> pair = feedPipeline.get(feedId);
        sourceFeedJoint = null;
        for (IFeedJoint joint : pair.second) {
            if (joint.getType().equals(IFeedJoint.FeedJointType.INTAKE)) {
                sourceFeedJoint = joint;
                break;
            }
        }

        if (sourceFeedJoint != null) {
            FeedIntakeInfo intakeJobInfo =
                    new FeedIntakeInfo(jobId, ActivityState.CREATED, feedId, sourceFeedJoint, jobSpec);
            pair.first.setFeedJobInfo(intakeJobInfo);
            entity2Intake.put(feedId, intakeJobInfo);
            jobs.put(jobId.getId(), intakeJobInfo);
            intakeJobs.put(jobId.getId(), intakeJobInfo);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Registered feed intake [" + jobId + "]" + " for feed " + feedId);
            }
        } else {
            throw new HyracksDataException(
                    "Could not register feed intake job [" + jobId + "]" + " for feed  " + feedId);
        }
    }

    public synchronized void registerFeedCollectionJob(EntityId sourceFeedId, FeedConnectionId connectionId,
            JobId jobId, JobSpecification jobSpec, Map<String, String> feedPolicy) {
        if (jobs.get(jobId.getId()) != null) {
            throw new IllegalStateException("Feed job already registered");
        }
        if (connectJobInfos.containsKey(jobId.getId())) {
            throw new IllegalStateException("Feed job already registered");
        }

        List<IFeedJoint> feedJoints = feedPipeline.get(sourceFeedId).second;
        FeedConnectionId cid = null;
        IFeedJoint collectionSourceFeedJoint = null;
        for (IFeedJoint joint : feedJoints) {
            cid = joint.getReceiver(connectionId);
            if (cid != null) {
                collectionSourceFeedJoint = joint;
                break;
            }
        }

        if (cid != null) {
            FeedConnectJobInfo cInfo = new FeedConnectJobInfo(sourceFeedId, jobId, ActivityState.CREATED, connectionId,
                    collectionSourceFeedJoint, null, jobSpec, feedPolicy);
            jobs.put(jobId.getId(), cInfo);
            connectJobInfos.put(connectionId, cInfo);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Registered feed connection [" + jobId + "]" + " for feed " + connectionId);
            }
        } else {
            LOGGER.warn(
                    "Could not register feed collection job [" + jobId + "]" + " for feed connection " + connectionId);
        }
    }

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec) {
        FeedConnectionId feedConnectionId = null;
        Map<String, String> feedPolicy = null;
        try {
            for (IOperatorDescriptor opDesc : spec.getOperatorMap().values()) {
                if (opDesc instanceof FeedCollectOperatorDescriptor) {
                    feedConnectionId = ((FeedCollectOperatorDescriptor) opDesc).getFeedConnectionId();
                    feedPolicy = ((FeedCollectOperatorDescriptor) opDesc).getFeedPolicyProperties();
                    registerFeedCollectionJob(((FeedCollectOperatorDescriptor) opDesc).getSourceFeedId(),
                            feedConnectionId, jobId, spec, feedPolicy);
                    return;
                } else if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                    registerFeedIntakeJob(((FeedIntakeOperatorDescriptor) opDesc).getFeedId(), jobId, spec);
                    return;
                }
            }
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    public synchronized List<String> getConnectionLocations(IFeedJoint feedJoint, final FeedConnectionRequest request)
            throws Exception {
        List<String> locations = null;
        switch (feedJoint.getType()) {
            case COMPUTE:
                FeedConnectionId connectionId = feedJoint.getProvider();
                FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
                locations = cInfo.getComputeLocations();
                break;
            case INTAKE:
                FeedIntakeInfo intakeInfo = entity2Intake.get(feedJoint.getOwnerFeedId());
                locations = intakeInfo.getIntakeLocation();
                break;
            default:
                break;
        }
        return locations;
    }

    private synchronized void notifyFeedEventSubscribers(FeedLifecycleEvent event) {
        if (subscribers != null && !subscribers.isEmpty()) {
            for (IFeedLifecycleEventSubscriber subscriber : subscribers) {
                subscriber.handleFeedEvent(event);
            }
        }
    }

    private synchronized void handleFeedIntakeJobFinishMessage(FeedIntakeInfo intakeInfo, ActiveEvent message)
            throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.INSTANCE.getHcc();
        JobInfo info = hcc.getJobInfo(message.getJobId());
        JobStatus status = info.getStatus();
        EntityId feedId = intakeInfo.getFeedId();
        Pair<FeedOperationCounter, List<IFeedJoint>> pair = feedPipeline.get(feedId);
        if (status.equals(JobStatus.FAILURE)) {
            pair.first.setFailedIngestion(true);
        }
        // remove feed joints
        deregisterFeedIntakeJob(message.getJobId());
        // notify event listeners
        feedPipeline.remove(feedId);
        entity2Intake.remove(feedId);
        notifyFeedEventSubscribers(pair.first.isFailedIngestion() ? FeedLifecycleEvent.FEED_INTAKE_FAILURE
                : FeedLifecycleEvent.FEED_INTAKE_ENDED);
    }

    private synchronized void handleFeedCollectJobFinishMessage(FeedConnectJobInfo cInfo) throws Exception {
        FeedConnectionId connectionId = cInfo.getConnectionId();

        IHyracksClientConnection hcc = AsterixAppContextInfo.INSTANCE.getHcc();
        JobInfo info = hcc.getJobInfo(cInfo.getJobId());
        JobStatus status = info.getStatus();
        boolean failure = status != null && status.equals(JobStatus.FAILURE);
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(cInfo.getFeedPolicy());
        boolean retainSubsription =
                cInfo.getState().equals(ActivityState.UNDER_RECOVERY) || (failure && fpa.continueOnHardwareFailure());

        if (!retainSubsription) {
            IFeedJoint feedJoint = cInfo.getSourceFeedJoint();
            feedJoint.removeReceiver(connectionId);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(
                        "Subscription " + cInfo.getConnectionId() + " completed successfully. Removed subscription");
            }
        }

        connectJobInfos.remove(connectionId);
        jobs.remove(cInfo.getJobId().getId());
        // notify event listeners
        FeedLifecycleEvent event =
                failure ? FeedLifecycleEvent.FEED_COLLECT_FAILURE : FeedLifecycleEvent.FEED_COLLECT_ENDED;
        notifyFeedEventSubscribers(event);
    }

    public List<String> getFeedStorageLocations(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getStorageLocations();
    }

    public List<String> getFeedCollectLocations(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getCollectLocations();
    }

    public List<String> getFeedIntakeLocations(EntityId feedId) {
        return entity2Intake.get(feedId).getIntakeLocation();
    }

    public JobId getFeedCollectJobId(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getJobId();
    }

    public boolean isFeedPointAvailable(FeedJointKey feedJointKey) {
        List<IFeedJoint> joints = feedPipeline.containsKey(feedJointKey.getFeedId())
                ? feedPipeline.get(feedJointKey.getFeedId()).second : null;
        if (joints != null && !joints.isEmpty()) {
            for (IFeedJoint joint : joints) {
                if (joint.getFeedJointKey().equals(feedJointKey)) {
                    return true;
                }
            }
        }
        return false;
    }

    public Collection<IFeedJoint> getFeedIntakeJoints() {
        List<IFeedJoint> intakeFeedPoints = new ArrayList<>();
        for (FeedIntakeInfo info : entity2Intake.values()) {
            intakeFeedPoints.add(info.getIntakeFeedJoint());
        }
        return intakeFeedPoints;
    }

    public IFeedJoint getFeedJoint(FeedJointKey feedPointKey) {
        List<IFeedJoint> joints = feedPipeline.containsKey(feedPointKey.getFeedId())
                ? feedPipeline.get(feedPointKey.getFeedId()).second : null;
        if (joints != null && !joints.isEmpty()) {
            for (IFeedJoint joint : joints) {
                if (joint.getFeedJointKey().equals(feedPointKey)) {
                    return joint;
                }
            }
        }
        return null;
    }

    public IFeedJoint getAvailableFeedJoint(FeedJointKey feedJointKey) {
        IFeedJoint feedJoint = getFeedJoint(feedJointKey);
        if (feedJoint != null) {
            return feedJoint;
        } else {
            String jointKeyString = feedJointKey.getStringRep();
            List<IFeedJoint> jointsOnPipeline = feedPipeline.containsKey(feedJointKey.getFeedId())
                    ? feedPipeline.get(feedJointKey.getFeedId()).second : null;
            IFeedJoint candidateJoint = null;
            if (jointsOnPipeline != null) {
                for (IFeedJoint joint : jointsOnPipeline) {
                    if (jointKeyString.contains(joint.getFeedJointKey().getStringRep()) && (candidateJoint == null
                            || /*found feed point is a super set of the earlier find*/joint.getFeedJointKey()
                                    .getStringRep().contains(candidateJoint.getFeedJointKey().getStringRep()))) {
                        candidateJoint = joint;
                    }
                }
            }
            return candidateJoint;
        }
    }

    public JobSpecification getCollectJobSpecification(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getSpec();
    }

    public IFeedJoint getFeedPoint(EntityId sourceFeedId, IFeedJoint.FeedJointType type) {
        List<IFeedJoint> joints = feedPipeline.get(sourceFeedId).second;
        for (IFeedJoint joint : joints) {
            if (joint.getType().equals(type)) {
                return joint;
            }
        }
        return null;
    }

    private void setLocations(FeedConnectJobInfo cInfo) {
        JobSpecification jobSpec = cInfo.getSpec();

        List<OperatorDescriptorId> collectOperatorIds = new ArrayList<>();
        List<OperatorDescriptorId> computeOperatorIds = new ArrayList<>();
        List<OperatorDescriptorId> storageOperatorIds = new ArrayList<>();

        Map<OperatorDescriptorId, IOperatorDescriptor> operators = jobSpec.getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            IOperatorDescriptor actualOp;
            if (opDesc instanceof FeedMetaOperatorDescriptor) {
                actualOp = ((FeedMetaOperatorDescriptor) opDesc).getCoreOperator();
            } else {
                actualOp = opDesc;
            }

            if (actualOp instanceof AlgebricksMetaOperatorDescriptor) {
                AlgebricksMetaOperatorDescriptor op = (AlgebricksMetaOperatorDescriptor) actualOp;
                IPushRuntimeFactory[] runtimeFactories = op.getPipeline().getRuntimeFactories();
                boolean computeOp = false;
                for (IPushRuntimeFactory rf : runtimeFactories) {
                    if (rf instanceof AssignRuntimeFactory) {
                        IConnectorDescriptor connDesc = jobSpec.getOperatorInputMap().get(op.getOperatorId()).get(0);
                        IOperatorDescriptor sourceOp =
                                jobSpec.getConnectorOperatorMap().get(connDesc.getConnectorId()).getLeft().getLeft();
                        if (sourceOp instanceof FeedCollectOperatorDescriptor) {
                            computeOp = true;
                            break;
                        }
                    }
                }
                if (computeOp) {
                    computeOperatorIds.add(entry.getKey());
                }
            } else if (actualOp instanceof LSMTreeIndexInsertUpdateDeleteOperatorDescriptor) {
                storageOperatorIds.add(entry.getKey());
            } else if (actualOp instanceof FeedCollectOperatorDescriptor) {
                collectOperatorIds.add(entry.getKey());
            }
        }

        try {
            IHyracksClientConnection hcc = AsterixAppContextInfo.INSTANCE.getHcc();
            JobInfo info = hcc.getJobInfo(cInfo.getJobId());
            List<String> collectLocations = new ArrayList<>();
            for (OperatorDescriptorId collectOpId : collectOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(collectOpId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    collectLocations.add(operatorLocations.get(i));
                }
            }

            List<String> computeLocations = new ArrayList<>();
            for (OperatorDescriptorId computeOpId : computeOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(computeOpId);
                if (operatorLocations != null) {
                    int nOperatorInstances = operatorLocations.size();
                    for (int i = 0; i < nOperatorInstances; i++) {
                        computeLocations.add(operatorLocations.get(i));
                    }
                } else {
                    computeLocations.clear();
                    computeLocations.addAll(collectLocations);
                }
            }

            List<String> storageLocations = new ArrayList<>();
            for (OperatorDescriptorId storageOpId : storageOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(storageOpId);
                if (operatorLocations == null) {
                    continue;
                }
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    storageLocations.add(operatorLocations.get(i));
                }
            }
            cInfo.setCollectLocations(collectLocations);
            cInfo.setComputeLocations(computeLocations);
            cInfo.setStorageLocations(storageLocations);

        } catch (Exception e) {
            LOGGER.error("Error while setting feed active locations", e);
        }

    }

    public synchronized void registerFeedEventSubscriber(IFeedLifecycleEventSubscriber subscriber) {
        subscribers.add(subscriber);
    }

    public void deregisterFeedEventSubscriber(IFeedLifecycleEventSubscriber subscriber) {
        subscribers.remove(subscriber);
    }

    public synchronized boolean isFeedConnectionActive(FeedConnectionId connectionId,
            IFeedLifecycleEventSubscriber eventSubscriber) {
        boolean active = false;
        FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
        if (cInfo != null) {
            active = cInfo.getState().equals(ActivityState.ACTIVE);
        }
        if (active) {
            registerFeedEventSubscriber(eventSubscriber);
        }
        return active;
    }

    public FeedConnectJobInfo getFeedConnectJobInfo(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId);
    }

    private void handleCollectJobStartMessage(FeedConnectJobInfo cInfo) throws ACIDException {
        // set locations of feed sub-operations (intake, compute, store)
        setLocations(cInfo);
        Pair<FeedOperationCounter, List<IFeedJoint>> pair = feedPipeline.get(cInfo.getConnectionId().getFeedId());
        // activate joints
        List<IFeedJoint> joints = pair.second;
        for (IFeedJoint joint : joints) {
            if (joint.getProvider().equals(cInfo.getConnectionId())) {
                joint.setState(State.ACTIVE);
                if (joint.getType().equals(IFeedJoint.FeedJointType.COMPUTE)) {
                    cInfo.setComputeFeedJoint(joint);
                }
            }
        }
        cInfo.setState(ActivityState.ACTIVE);
    }

    private synchronized boolean isConnectedToDataset(String datasetName) {
        for (FeedConnectionId connection : connectJobInfos.keySet()) {
            if (connection.getDatasetName().equals(datasetName)) {
                return true;
            }
        }
        return false;
    }

    public FeedConnectionId[] getConnections() {
        return connectJobInfos.keySet().toArray(new FeedConnectionId[connectJobInfos.size()]);
    }

    public boolean isFeedJointAvailable(FeedJointKey feedJointKey) {
        return isFeedPointAvailable(feedJointKey);
    }

    @Override
    public boolean isEntityActive() {
        return !jobs.isEmpty();
    }

    @Override
    public EntityId getEntityId() {
        return entityId;
    }

    public IFeedJoint getSourceFeedJoint() {
        return sourceFeedJoint;
    }

    @Override
    public boolean isEntityConnectedToDataset(String dataverseName, String datasetName) {
        return isConnectedToDataset(datasetName);
    }
}
