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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.app.external.FeedLifecycleListener.FeedEvent;
import org.apache.asterix.app.external.FeedWorkCollection.SubscribeFeedWork;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.external.feed.api.FeedOperationCounter;
import org.apache.asterix.external.feed.api.IFeedJoint;
import org.apache.asterix.external.feed.api.IFeedJoint.State;
import org.apache.asterix.external.feed.api.IFeedLifecycleEventSubscriber;
import org.apache.asterix.external.feed.api.IFeedLifecycleEventSubscriber.FeedLifecycleEvent;
import org.apache.asterix.external.feed.api.IIntakeProgressTracker;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.management.FeedConnectionRequest;
import org.apache.asterix.external.feed.management.FeedId;
import org.apache.asterix.external.feed.management.FeedJointKey;
import org.apache.asterix.external.feed.management.FeedWorkManager;
import org.apache.asterix.external.feed.message.StorageReportFeedMessage;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedActivity;
import org.apache.asterix.external.feed.watch.FeedConnectJobInfo;
import org.apache.asterix.external.feed.watch.FeedIntakeInfo;
import org.apache.asterix.external.feed.watch.FeedJobInfo;
import org.apache.asterix.external.feed.watch.FeedJobInfo.FeedJobState;
import org.apache.asterix.external.feed.watch.FeedJobInfo.JobType;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.metadata.feeds.BuiltinFeedPolicies;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.commons.lang3.StringUtils;
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

public class FeedJobNotificationHandler implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(FeedJobNotificationHandler.class.getName());

    private final LinkedBlockingQueue<FeedEvent> inbox;
    private final Map<FeedConnectionId, List<IFeedLifecycleEventSubscriber>> eventSubscribers;
    private final Map<JobId, FeedJobInfo> jobInfos;
    private final Map<FeedId, FeedIntakeInfo> intakeJobInfos;
    private final Map<FeedConnectionId, FeedConnectJobInfo> connectJobInfos;
    private final Map<FeedId, Pair<FeedOperationCounter, List<IFeedJoint>>> feedPipeline;
    private final Map<FeedConnectionId, Pair<IIntakeProgressTracker, Long>> feedIntakeProgressTrackers;

    public FeedJobNotificationHandler(LinkedBlockingQueue<FeedEvent> inbox) {
        this.inbox = inbox;
        this.jobInfos = new HashMap<JobId, FeedJobInfo>();
        this.intakeJobInfos = new HashMap<FeedId, FeedIntakeInfo>();
        this.connectJobInfos = new HashMap<FeedConnectionId, FeedConnectJobInfo>();
        this.feedPipeline = new HashMap<FeedId, Pair<FeedOperationCounter, List<IFeedJoint>>>();
        this.eventSubscribers = new HashMap<FeedConnectionId, List<IFeedLifecycleEventSubscriber>>();
        this.feedIntakeProgressTrackers = new HashMap<FeedConnectionId, Pair<IIntakeProgressTracker, Long>>();
    }

    @Override
    public void run() {
        FeedEvent event = null;
        Thread.currentThread().setName("FeedJobNotificationHandler");
        while (true) {
            try {
                event = inbox.take();
                switch (event.eventKind) {
                    case JOB_START:
                        handleJobStartEvent(event);
                        break;
                    case JOB_FINISH:
                        handleJobFinishEvent(event);
                        break;
                    case PARTITION_START:
                        handlePartitionStart(event);
                        break;
                    default:
                        LOGGER.log(Level.WARNING, "Unknown Feed Event");
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void registerFeedIntakeProgressTracker(FeedConnectionId connectionId,
            IIntakeProgressTracker feedIntakeProgressTracker) {
        if (feedIntakeProgressTrackers.get(connectionId) == null) {
            this.feedIntakeProgressTrackers.put(connectionId,
                    new Pair<IIntakeProgressTracker, Long>(feedIntakeProgressTracker, 0L));
        } else {
            throw new IllegalStateException(
                    " Progress tracker for connection " + connectionId + " is alreader registered");
        }
    }

    public void deregisterFeedIntakeProgressTracker(FeedConnectionId connectionId) {
        this.feedIntakeProgressTrackers.remove(connectionId);
    }

    public void updateTrackingInformation(StorageReportFeedMessage srm) {
        Pair<IIntakeProgressTracker, Long> p = feedIntakeProgressTrackers.get(srm.getConnectionId());
        if (p != null && p.second < srm.getLastPersistedTupleIntakeTimestamp()) {
            p.second = srm.getLastPersistedTupleIntakeTimestamp();
            p.first.notifyIngestedTupleTimestamp(p.second);
        }
    }

    public Collection<FeedIntakeInfo> getFeedIntakeInfos() {
        return intakeJobInfos.values();
    }

    public Collection<FeedConnectJobInfo> getFeedConnectInfos() {
        return connectJobInfos.values();
    }

    public synchronized void registerFeedJoint(IFeedJoint feedJoint, int numOfPrividers) {
        Pair<FeedOperationCounter, List<IFeedJoint>> feedJointsOnPipeline = feedPipeline
                .get(feedJoint.getOwnerFeedId());

        if (feedJointsOnPipeline == null) {
            feedJointsOnPipeline = new Pair<FeedOperationCounter, List<IFeedJoint>>(
                    new FeedOperationCounter(numOfPrividers), new ArrayList<IFeedJoint>());
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

    public synchronized void registerFeedIntakeJob(FeedId feedId, JobId jobId, JobSpecification jobSpec)
            throws HyracksDataException {
        if (jobInfos.get(jobId) != null) {
            throw new IllegalStateException("Feed job already registered");
        }

        Pair<FeedOperationCounter, List<IFeedJoint>> pair = feedPipeline.containsKey(feedId) ? feedPipeline.get(feedId)
                : null;
        IFeedJoint intakeJoint = null;
        for (IFeedJoint joint : pair.second) {
            if (joint.getType().equals(IFeedJoint.FeedJointType.INTAKE)) {
                intakeJoint = joint;
                break;
            }
        }

        if (intakeJoint != null) {
            FeedIntakeInfo intakeJobInfo = new FeedIntakeInfo(jobId, FeedJobState.CREATED, FeedJobInfo.JobType.INTAKE,
                    feedId, intakeJoint, jobSpec);
            pair.first.setFeedJobInfo(intakeJobInfo);
            intakeJobInfos.put(feedId, intakeJobInfo);
            jobInfos.put(jobId, intakeJobInfo);

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed intake [" + jobId + "]" + " for feed " + feedId);
            }
        } else {
            throw new HyracksDataException(
                    "Could not register feed intake job [" + jobId + "]" + " for feed  " + feedId);
        }
    }

    public synchronized void registerFeedCollectionJob(FeedId sourceFeedId, FeedConnectionId connectionId, JobId jobId,
            JobSpecification jobSpec, Map<String, String> feedPolicy) {
        if (jobInfos.get(jobId) != null) {
            throw new IllegalStateException("Feed job already registered");
        }

        List<IFeedJoint> feedJoints = feedPipeline.get(sourceFeedId).second;
        FeedConnectionId cid = null;
        IFeedJoint sourceFeedJoint = null;
        for (IFeedJoint joint : feedJoints) {
            cid = joint.getReceiver(connectionId);
            if (cid != null) {
                sourceFeedJoint = joint;
                break;
            }
        }

        if (cid != null) {
            FeedConnectJobInfo cInfo = new FeedConnectJobInfo(jobId, FeedJobState.CREATED, connectionId,
                    sourceFeedJoint, null, jobSpec, feedPolicy);
            jobInfos.put(jobId, cInfo);
            connectJobInfos.put(connectionId, cInfo);

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed connection [" + jobId + "]" + " for feed " + connectionId);
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Could not register feed collection job [" + jobId + "]" + " for feed connection "
                        + connectionId);
            }
        }

    }

    public synchronized void deregisterFeedIntakeJob(JobId jobId) {
        if (jobInfos.get(jobId) == null) {
            throw new IllegalStateException(" Feed Intake job not registered ");
        }

        FeedIntakeInfo info = (FeedIntakeInfo) jobInfos.get(jobId);
        jobInfos.remove(jobId);
        intakeJobInfos.remove(info.getFeedId());

        if (!info.getState().equals(FeedJobState.UNDER_RECOVERY)) {
            List<IFeedJoint> joints = feedPipeline.get(info.getFeedId()).second;
            joints.remove(info.getIntakeFeedJoint());

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deregistered feed intake job [" + jobId + "]");
            }
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Not removing feed joint as intake job is in " + FeedJobState.UNDER_RECOVERY + " state.");
            }
        }

    }

    private synchronized void handleJobStartEvent(FeedEvent message) throws Exception {
        FeedJobInfo jobInfo = jobInfos.get(message.jobId);
        switch (jobInfo.getJobType()) {
            case INTAKE:
                handleIntakeJobStartMessage((FeedIntakeInfo) jobInfo);
                break;
            case FEED_CONNECT:
                handleCollectJobStartMessage((FeedConnectJobInfo) jobInfo);
                break;
        }

    }

    private synchronized void handleJobFinishEvent(FeedEvent message) throws Exception {
        FeedJobInfo jobInfo = jobInfos.get(message.jobId);
        switch (jobInfo.getJobType()) {
            case INTAKE:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Intake Job finished for feed intake " + jobInfo.getJobId());
                }
                handleFeedIntakeJobFinishMessage((FeedIntakeInfo) jobInfo, message);
                break;
            case FEED_CONNECT:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Collect Job finished for  " + jobInfo);
                }
                handleFeedCollectJobFinishMessage((FeedConnectJobInfo) jobInfo);
                break;
        }
    }

    private synchronized void handlePartitionStart(FeedEvent message) {
        FeedJobInfo jobInfo = jobInfos.get(message.jobId);
        switch (jobInfo.getJobType()) {
            case FEED_CONNECT:
                ((FeedConnectJobInfo) jobInfo).partitionStart();
                if (((FeedConnectJobInfo) jobInfo).collectionStarted()) {
                    notifyFeedEventSubscribers(jobInfo, FeedLifecycleEvent.FEED_COLLECT_STARTED);
                }
                break;
            case INTAKE:
                Pair<FeedOperationCounter, List<IFeedJoint>> feedCounter = feedPipeline.get(message.feedId);
                feedCounter.first.setPartitionCount(feedCounter.first.getPartitionCount() - 1);;
                if (feedCounter.first.getPartitionCount() == 0) {
                    ((FeedIntakeInfo) jobInfo).getIntakeFeedJoint().setState(State.ACTIVE);
                    jobInfo.setState(FeedJobState.ACTIVE);
                    notifyFeedEventSubscribers(jobInfo, FeedLifecycleEvent.FEED_INTAKE_STARTED);
                }
                break;
            default:
                break;

        }
    }

    private synchronized void handleIntakeJobStartMessage(FeedIntakeInfo intakeJobInfo) throws Exception {
        List<OperatorDescriptorId> intakeOperatorIds = new ArrayList<OperatorDescriptorId>();
        Map<OperatorDescriptorId, IOperatorDescriptor> operators = intakeJobInfo.getSpec().getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                intakeOperatorIds.add(opDesc.getOperatorId());
            }
        }

        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(intakeJobInfo.getJobId());
        List<String> intakeLocations = new ArrayList<String>();
        for (OperatorDescriptorId intakeOperatorId : intakeOperatorIds) {
            Map<Integer, String> operatorLocations = info.getOperatorLocations().get(intakeOperatorId);
            int nOperatorInstances = operatorLocations.size();
            for (int i = 0; i < nOperatorInstances; i++) {
                intakeLocations.add(operatorLocations.get(i));
            }
        }
        // intakeLocations is an ordered list; element at position i corresponds to location of i'th instance of operator
        intakeJobInfo.setIntakeLocation(intakeLocations);
    }

    private void handleCollectJobStartMessage(FeedConnectJobInfo cInfo) throws RemoteException, ACIDException {
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
        cInfo.setState(FeedJobState.ACTIVE);
        // register activity in metadata
        registerFeedActivity(cInfo);
    }

    private void notifyFeedEventSubscribers(FeedJobInfo jobInfo, FeedLifecycleEvent event) {
        JobType jobType = jobInfo.getJobType();
        List<FeedConnectionId> impactedConnections = new ArrayList<FeedConnectionId>();
        if (jobType.equals(JobType.INTAKE)) {
            FeedId feedId = ((FeedIntakeInfo) jobInfo).getFeedId();
            for (FeedConnectionId connId : eventSubscribers.keySet()) {
                if (connId.getFeedId().equals(feedId)) {
                    impactedConnections.add(connId);
                }
            }
        } else {
            impactedConnections.add(((FeedConnectJobInfo) jobInfo).getConnectionId());
        }

        for (FeedConnectionId connId : impactedConnections) {
            List<IFeedLifecycleEventSubscriber> subscribers = eventSubscribers.get(connId);
            if (subscribers != null && !subscribers.isEmpty()) {
                for (IFeedLifecycleEventSubscriber subscriber : subscribers) {
                    subscriber.handleFeedEvent(event);
                }
            }
            if (event == FeedLifecycleEvent.FEED_COLLECT_ENDED) {
                eventSubscribers.remove(connId);
            }
        }
    }

    public synchronized void submitFeedConnectionRequest(IFeedJoint feedJoint, final FeedConnectionRequest request)
            throws Exception {
        List<String> locations = null;
        switch (feedJoint.getType()) {
            case INTAKE:
                FeedIntakeInfo intakeInfo = intakeJobInfos.get(feedJoint.getOwnerFeedId());
                locations = intakeInfo.getIntakeLocation();
                break;
            case COMPUTE:
                FeedConnectionId connectionId = feedJoint.getProvider();
                FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
                locations = cInfo.getComputeLocations();
                break;
        }

        SubscribeFeedWork work = new SubscribeFeedWork(locations.toArray(new String[] {}), request);
        FeedWorkManager.INSTANCE.submitWork(work, new SubscribeFeedWork.FeedSubscribeWorkEventListener());
    }

    public IFeedJoint getSourceFeedJoint(FeedConnectionId connectionId) {
        FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
        if (cInfo != null) {
            return cInfo.getSourceFeedJoint();
        }
        return null;
    }

    public Set<FeedConnectionId> getActiveFeedConnections() {
        Set<FeedConnectionId> activeConnections = new HashSet<FeedConnectionId>();
        for (FeedConnectJobInfo cInfo : connectJobInfos.values()) {
            if (cInfo.getState().equals(FeedJobState.ACTIVE)) {
                activeConnections.add(cInfo.getConnectionId());
            }
        }
        return activeConnections;
    }

    public synchronized boolean isFeedConnectionActive(FeedConnectionId connectionId,
            IFeedLifecycleEventSubscriber eventSubscriber) {
        boolean active = false;
        FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
        if (cInfo != null) {
            active = cInfo.getState().equals(FeedJobState.ACTIVE);
        }
        if (active) {
            registerFeedEventSubscriber(connectionId, eventSubscriber);
        }
        return active;
    }

    public void setJobState(FeedConnectionId connectionId, FeedJobState jobState) {
        FeedConnectJobInfo connectJobInfo = connectJobInfos.get(connectionId);
        connectJobInfo.setState(jobState);
    }

    public FeedJobState getFeedJobState(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getState();
    }

    private synchronized void handleFeedIntakeJobFinishMessage(FeedIntakeInfo intakeInfo, FeedEvent message)
            throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(message.jobId);
        JobStatus status = info.getStatus();
        FeedId feedId = intakeInfo.getFeedId();
        Pair<FeedOperationCounter, List<IFeedJoint>> pair = feedPipeline.get(feedId);
        if (status.equals(JobStatus.FAILURE)) {
            pair.first.setFailedIngestion(true);
        }
        // remove feed joints
        deregisterFeedIntakeJob(message.jobId);
        // notify event listeners
        feedPipeline.remove(feedId);
        intakeJobInfos.remove(feedId);
        notifyFeedEventSubscribers(intakeInfo, pair.first.isFailedIngestion() ? FeedLifecycleEvent.FEED_INTAKE_FAILURE
                : FeedLifecycleEvent.FEED_INTAKE_ENDED);
    }

    private synchronized void handleFeedCollectJobFinishMessage(FeedConnectJobInfo cInfo) throws Exception {
        FeedConnectionId connectionId = cInfo.getConnectionId();

        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(cInfo.getJobId());
        JobStatus status = info.getStatus();
        boolean failure = status != null && status.equals(JobStatus.FAILURE);
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(cInfo.getFeedPolicy());
        boolean retainSubsription = cInfo.getState().equals(FeedJobState.UNDER_RECOVERY)
                || (failure && fpa.continueOnHardwareFailure());

        if (!retainSubsription) {
            IFeedJoint feedJoint = cInfo.getSourceFeedJoint();
            feedJoint.removeReceiver(connectionId);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(
                        "Subscription " + cInfo.getConnectionId() + " completed successfully. Removed subscription");
            }
        }

        connectJobInfos.remove(connectionId);
        jobInfos.remove(cInfo.getJobId());
        feedIntakeProgressTrackers.remove(cInfo.getConnectionId());
        deregisterFeedActivity(cInfo);
        // notify event listeners
        FeedLifecycleEvent event = failure ? FeedLifecycleEvent.FEED_COLLECT_FAILURE
                : FeedLifecycleEvent.FEED_COLLECT_ENDED;
        notifyFeedEventSubscribers(cInfo, event);
    }

    private void registerFeedActivity(FeedConnectJobInfo cInfo) {
        Map<String, String> feedActivityDetails = new HashMap<String, String>();

        if (cInfo.getCollectLocations() != null) {
            feedActivityDetails.put(FeedActivity.FeedActivityDetails.INTAKE_LOCATIONS,
                    StringUtils.join(cInfo.getCollectLocations().iterator(), ','));
        }

        if (cInfo.getComputeLocations() != null) {
            feedActivityDetails.put(FeedActivity.FeedActivityDetails.COMPUTE_LOCATIONS,
                    StringUtils.join(cInfo.getComputeLocations().iterator(), ','));
        }

        if (cInfo.getStorageLocations() != null) {
            feedActivityDetails.put(FeedActivity.FeedActivityDetails.STORAGE_LOCATIONS,
                    StringUtils.join(cInfo.getStorageLocations().iterator(), ','));
        }

        String policyName = cInfo.getFeedPolicy().get(BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY);
        feedActivityDetails.put(FeedActivity.FeedActivityDetails.FEED_POLICY_NAME, policyName);

        feedActivityDetails.put(FeedActivity.FeedActivityDetails.FEED_CONNECT_TIMESTAMP, (new Date()).toString());
        try {
            FeedActivity feedActivity = new FeedActivity(cInfo.getConnectionId().getFeedId().getDataverse(),
                    cInfo.getConnectionId().getFeedId().getFeedName(), cInfo.getConnectionId().getDatasetName(),
                    feedActivityDetails);
            CentralFeedManager.getInstance().getFeedLoadManager().reportFeedActivity(cInfo.getConnectionId(),
                    feedActivity);

        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to register feed activity for " + cInfo + " " + e.getMessage());
            }

        }

    }

    public void deregisterFeedActivity(FeedConnectJobInfo cInfo) {
        try {
            CentralFeedManager.getInstance().getFeedLoadManager().removeFeedActivity(cInfo.getConnectionId());
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to deregister feed activity for " + cInfo + " " + e.getMessage());
            }
        }
    }

    public boolean isRegisteredFeedJob(JobId jobId) {
        return jobInfos.get(jobId) != null;
    }

    public List<String> getFeedComputeLocations(FeedId feedId) {
        List<IFeedJoint> feedJoints = feedPipeline.get(feedId).second;
        for (IFeedJoint joint : feedJoints) {
            if (joint.getFeedJointKey().getFeedId().equals(feedId)) {
                return connectJobInfos.get(joint.getProvider()).getComputeLocations();
            }
        }
        return null;
    }

    public List<String> getFeedStorageLocations(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getStorageLocations();
    }

    public List<String> getFeedCollectLocations(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getCollectLocations();
    }

    public List<String> getFeedIntakeLocations(FeedId feedId) {
        return intakeJobInfos.get(feedId).getIntakeLocation();
    }

    public JobId getFeedCollectJobId(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getJobId();
    }

    public synchronized void registerFeedEventSubscriber(FeedConnectionId connectionId,
            IFeedLifecycleEventSubscriber subscriber) {
        List<IFeedLifecycleEventSubscriber> subscribers = eventSubscribers.get(connectionId);
        if (subscribers == null) {
            subscribers = new ArrayList<IFeedLifecycleEventSubscriber>();
            eventSubscribers.put(connectionId, subscribers);
        }
        subscribers.add(subscriber);
    }

    public void deregisterFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber) {
        List<IFeedLifecycleEventSubscriber> subscribers = eventSubscribers.get(connectionId);
        if (subscribers != null) {
            subscribers.remove(subscriber);
        }
    }

    // ============================

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
        List<IFeedJoint> intakeFeedPoints = new ArrayList<IFeedJoint>();
        for (FeedIntakeInfo info : intakeJobInfos.values()) {
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
                    if (jointKeyString.contains(joint.getFeedJointKey().getStringRep())) {
                        if (candidateJoint == null) {
                            candidateJoint = joint;
                        } else if (joint.getFeedJointKey().getStringRep()
                                .contains(candidateJoint.getFeedJointKey().getStringRep())) { // found feed point is a super set of the earlier find
                            candidateJoint = joint;
                        }
                    }
                }
            }
            return candidateJoint;
        }
    }

    public JobSpecification getCollectJobSpecification(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getSpec();
    }

    public IFeedJoint getFeedPoint(FeedId sourceFeedId, IFeedJoint.FeedJointType type) {
        List<IFeedJoint> joints = feedPipeline.get(sourceFeedId).second;
        for (IFeedJoint joint : joints) {
            if (joint.getType().equals(type)) {
                return joint;
            }
        }
        return null;
    }

    public FeedConnectJobInfo getFeedConnectJobInfo(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId);
    }

    private void setLocations(FeedConnectJobInfo cInfo) {
        JobSpecification jobSpec = cInfo.getSpec();

        List<OperatorDescriptorId> collectOperatorIds = new ArrayList<OperatorDescriptorId>();
        List<OperatorDescriptorId> computeOperatorIds = new ArrayList<OperatorDescriptorId>();
        List<OperatorDescriptorId> storageOperatorIds = new ArrayList<OperatorDescriptorId>();

        Map<OperatorDescriptorId, IOperatorDescriptor> operators = jobSpec.getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            IOperatorDescriptor actualOp = null;
            if (opDesc instanceof FeedMetaOperatorDescriptor) {
                actualOp = ((FeedMetaOperatorDescriptor) opDesc).getCoreOperator();
            } else {
                actualOp = opDesc;
            }

            if (actualOp instanceof AlgebricksMetaOperatorDescriptor) {
                AlgebricksMetaOperatorDescriptor op = ((AlgebricksMetaOperatorDescriptor) actualOp);
                IPushRuntimeFactory[] runtimeFactories = op.getPipeline().getRuntimeFactories();
                boolean computeOp = false;
                for (IPushRuntimeFactory rf : runtimeFactories) {
                    if (rf instanceof AssignRuntimeFactory) {
                        IConnectorDescriptor connDesc = jobSpec.getOperatorInputMap().get(op.getOperatorId()).get(0);
                        IOperatorDescriptor sourceOp = jobSpec.getConnectorOperatorMap().get(connDesc.getConnectorId())
                                .getLeft().getLeft();
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
            IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
            JobInfo info = hcc.getJobInfo(cInfo.getJobId());
            List<String> collectLocations = new ArrayList<String>();
            for (OperatorDescriptorId collectOpId : collectOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(collectOpId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    collectLocations.add(operatorLocations.get(i));
                }
            }

            List<String> computeLocations = new ArrayList<String>();
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

            List<String> storageLocations = new ArrayList<String>();
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
            e.printStackTrace();
        }

    }
}
