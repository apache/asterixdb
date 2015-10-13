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
package org.apache.asterix.feeds;

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

import org.apache.asterix.api.common.FeedWorkCollection.SubscribeFeedWork;
import org.apache.asterix.common.active.ActiveObjectId;
import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.ActiveJobInfo;
import org.apache.asterix.common.active.ActiveJobInfo.ActiveJopType;
import org.apache.asterix.common.active.ActiveJobInfo.JobState;
import org.apache.asterix.common.channels.ChannelActivity;
import org.apache.asterix.common.channels.ChannelJobInfo;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.feeds.FeedActivity;
import org.apache.asterix.common.feeds.FeedConnectJobInfo;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConnectionRequest;
import org.apache.asterix.common.feeds.FeedIntakeInfo;
import org.apache.asterix.common.feeds.FeedJointKey;
import org.apache.asterix.common.feeds.FeedPolicyAccessor;
import org.apache.asterix.common.feeds.api.IActiveLifecycleEventSubscriber;
import org.apache.asterix.common.feeds.api.IActiveLifecycleEventSubscriber.ActiveLifecycleEvent;
import org.apache.asterix.common.feeds.api.IFeedJoint;
import org.apache.asterix.common.feeds.api.IFeedJoint.State;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.common.feeds.message.StorageReportFeedMessage;
import org.apache.asterix.feeds.ActiveJobLifecycleListener.Message;
import org.apache.asterix.metadata.channels.ChannelMetaOperatorDescriptor;
import org.apache.asterix.metadata.channels.RepetitiveChannelOperatorDescriptor;
import org.apache.asterix.metadata.feeds.BuiltinFeedPolicies;
import org.apache.asterix.metadata.feeds.FeedCollectOperatorDescriptor;
import org.apache.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
import org.apache.asterix.metadata.feeds.FeedMetaOperatorDescriptor;
import org.apache.asterix.metadata.feeds.FeedWorkManager;
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

public class ActiveJobNotificationHandler implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ActiveJobNotificationHandler.class.getName());

    private final LinkedBlockingQueue<Message> inbox;
    private final Map<ActiveJobId, List<IActiveLifecycleEventSubscriber>> eventSubscribers;

    private final Map<JobId, ActiveJobInfo> jobInfos;
    private final Map<ActiveObjectId, FeedIntakeInfo> intakeJobInfos;
    private final Map<ActiveJobId, ActiveJobInfo> activeJobInfos;
    private final Map<FeedConnectionId, FeedConnectJobInfo> connectJobInfos;
    private final Map<ActiveObjectId, List<IFeedJoint>> feedPipeline;
    private final Map<FeedConnectionId, Pair<IIntakeProgressTracker, Long>> feedIntakeProgressTrackers;

    public ActiveJobNotificationHandler(LinkedBlockingQueue<Message> inbox) {
        this.inbox = inbox;
        this.jobInfos = new HashMap<JobId, ActiveJobInfo>();
        this.intakeJobInfos = new HashMap<ActiveObjectId, FeedIntakeInfo>();
        this.connectJobInfos = new HashMap<FeedConnectionId, FeedConnectJobInfo>();
        this.activeJobInfos = new HashMap<ActiveJobId, ActiveJobInfo>();
        this.feedPipeline = new HashMap<ActiveObjectId, List<IFeedJoint>>();
        this.eventSubscribers = new HashMap<ActiveJobId, List<IActiveLifecycleEventSubscriber>>();
        this.feedIntakeProgressTrackers = new HashMap<FeedConnectionId, Pair<IIntakeProgressTracker, Long>>();
    }

    @Override
    public void run() {
        Message mesg;
        while (true) {
            try {
                mesg = inbox.take();
                switch (mesg.messageKind) {
                    case JOB_START:
                        handleJobStartMessage(mesg);
                        break;
                    case JOB_FINISH:
                        handleJobFinishMessage(mesg);
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
            this.feedIntakeProgressTrackers.put(connectionId, new Pair<IIntakeProgressTracker, Long>(
                    feedIntakeProgressTracker, 0L));
        } else {
            throw new IllegalStateException(" Progress tracker for connection " + connectionId
                    + " is alreader registered");
        }
    }

    public void deregisterFeedIntakeProgressTracker(ActiveJobId connectionId) {
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

    public Collection<ActiveJobInfo> getActiveJobInfos() {
        return activeJobInfos.values();
    }

    public void registerFeedJoint(IFeedJoint feedJoint) {
        List<IFeedJoint> feedJointsOnPipeline = feedPipeline.get(feedJoint.getOwnerFeedId());
        if (feedJointsOnPipeline == null) {
            feedJointsOnPipeline = new ArrayList<IFeedJoint>();
            feedPipeline.put(feedJoint.getOwnerFeedId(), feedJointsOnPipeline);
            feedJointsOnPipeline.add(feedJoint);
        } else {
            if (!feedJointsOnPipeline.contains(feedJoint)) {
                feedJointsOnPipeline.add(feedJoint);
            } else {
                throw new IllegalArgumentException("Feed joint " + feedJoint + " already registered");
            }
        }
    }

    public void registerActiveJob(ActiveJobId activeJobId, JobId jobId, ActiveJopType jobType, JobSpecification jobSpec) {
        if (jobInfos.get(jobId) != null) {
            throw new IllegalStateException("Active job already registered");
        }

        ActiveJobInfo cInfo = new ActiveJobInfo(jobId, JobState.CREATED, jobType, jobSpec, activeJobId);
        jobInfos.put(jobId, cInfo);
        activeJobInfos.put(activeJobId, cInfo);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Registered active job [" + jobId + "]" + " for activejob " + activeJobId);
        }

    }

    public void registerFeedIntakeJob(ActiveObjectId feedId, JobId jobId, JobSpecification jobSpec)
            throws HyracksDataException {
        if (jobInfos.get(jobId) != null) {
            throw new IllegalStateException("Feed job already registered");
        }

        List<IFeedJoint> joints = feedPipeline.get(feedId);
        IFeedJoint intakeJoint = null;
        for (IFeedJoint joint : joints) {
            if (joint.getType().equals(IFeedJoint.FeedJointType.INTAKE)) {
                intakeJoint = joint;
                break;
            }
        }

        if (intakeJoint != null) {
            FeedIntakeInfo intakeJobInfo = new FeedIntakeInfo(jobId, JobState.CREATED, feedId, intakeJoint, jobSpec);
            intakeJobInfos.put(feedId, intakeJobInfo);
            jobInfos.put(jobId, intakeJobInfo);

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed intake [" + jobId + "]" + " for feed " + feedId);
            }
        } else {
            throw new HyracksDataException("Could not register feed intake job [" + jobId + "]" + " for feed  "
                    + feedId);
        }
    }

    public void registerFeedCollectionJob(ActiveObjectId sourceFeedId, FeedConnectionId connectionId, JobId jobId,
            JobSpecification jobSpec, Map<String, String> feedPolicy) {
        if (jobInfos.get(jobId) != null) {
            throw new IllegalStateException("Feed job already registered");
        }

        List<IFeedJoint> feedJoints = feedPipeline.get(sourceFeedId);
        ActiveJobId cid = null;
        IFeedJoint sourceFeedJoint = null;
        for (IFeedJoint joint : feedJoints) {
            cid = joint.getReceiver(connectionId);
            if (cid != null) {
                sourceFeedJoint = joint;
                break;
            }
        }

        if (cid != null) {
            FeedConnectJobInfo cInfo = new FeedConnectJobInfo(jobId, JobState.CREATED, connectionId, sourceFeedJoint,
                    null, jobSpec, feedPolicy);
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

    public void deregisterActiveJob(JobId jobId) {
        if (jobInfos.get(jobId) == null) {
            throw new IllegalStateException(" Active job not registered ");
        }

        ActiveJobInfo info = jobInfos.get(jobId);
        jobInfos.remove(jobId);
        activeJobInfos.remove(info.getActiveJobId());

        if (!info.getState().equals(JobState.UNDER_RECOVERY)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deregistered active job [" + jobId + "]");
            }
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Not removing active as job is in " + JobState.UNDER_RECOVERY + " state.");
            }
        }

    }

    public void deregisterFeedIntakeJob(JobId jobId) {
        if (jobInfos.get(jobId) == null) {
            throw new IllegalStateException(" Feed Intake job not registered ");
        }

        FeedIntakeInfo info = (FeedIntakeInfo) jobInfos.get(jobId);
        jobInfos.remove(jobId);
        intakeJobInfos.remove(info.getFeedId());

        if (!info.getState().equals(JobState.UNDER_RECOVERY)) {
            List<IFeedJoint> joints = feedPipeline.get(info.getFeedId());
            joints.remove(info.getIntakeFeedJoint());

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deregistered feed intake job [" + jobId + "]");
            }
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Not removing feed joint as intake job is in " + JobState.UNDER_RECOVERY + " state.");
            }
        }

    }

    private void handleJobStartMessage(Message message) throws Exception {
        ActiveJobInfo jobInfo = jobInfos.get(message.jobId);
        switch (jobInfo.getJobType()) {
            case FEED_INTAKE:
                handleIntakeJobStartMessage((FeedIntakeInfo) jobInfo);
                break;
            case FEED_CONNECT:
                handleCollectJobStartMessage((FeedConnectJobInfo) jobInfo);
                break;
            case FEED_COLLECT:
                break;
            default:
                handleActiveJobStartMessage(jobInfo);
                break;
        }
    }

    private void handleJobFinishMessage(Message message) throws Exception {
        ActiveJobInfo jobInfo = jobInfos.get(message.jobId);
        switch (jobInfo.getJobType()) {
            case FEED_INTAKE:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Intake Job finished for feed intake " + jobInfo.getJobId());
                }
                handleFeedIntakeJobFinishMessage((FeedIntakeInfo) jobInfo, message);
                break;
            case FEED_CONNECT:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Collect Job finished for  " + (FeedConnectJobInfo) jobInfo);
                }
                handleFeedCollectJobFinishMessage((FeedConnectJobInfo) jobInfo);
                break;
            case FEED_COLLECT:
                break;
            default:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Active Job finished for " + jobInfo.getJobId());
                }
                handleActiveJobFinishMessage(jobInfo, message);
                break;
        }

    }

    private synchronized void handleActiveJobStartMessage(ActiveJobInfo jobInfo) throws Exception {
        setLocations((ChannelJobInfo) jobInfo);
        jobInfo.setState(JobState.ACTIVE);
        // notify event listeners 
        notifyActiveEventSubscribers(jobInfo, ActiveLifecycleEvent.ACTIVE_JOB_STARTED);
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
        intakeJobInfo.getIntakeFeedJoint().setState(State.ACTIVE);
        intakeJobInfo.setState(JobState.ACTIVE);

        // notify event listeners 
        notifyFeedEventSubscribers(intakeJobInfo, ActiveLifecycleEvent.FEED_INTAKE_STARTED);
    }

    private void handleCollectJobStartMessage(FeedConnectJobInfo cInfo) throws RemoteException, ACIDException {
        // set locations of feed sub-operations (intake, compute, store)
        setLocations(cInfo);

        // activate joints
        List<IFeedJoint> joints = feedPipeline.get(cInfo.getConnectionId().getActiveId());
        for (IFeedJoint joint : joints) {
            if (joint.getProvider().equals(cInfo.getConnectionId())) {
                joint.setState(State.ACTIVE);
                if (joint.getType().equals(IFeedJoint.FeedJointType.COMPUTE)) {
                    cInfo.setComputeFeedJoint(joint);
                }
            }
        }
        cInfo.setState(JobState.ACTIVE);

        // register activity in metadata
        registerFeedActivity(cInfo);
        // notify event listeners
        notifyFeedEventSubscribers(cInfo, ActiveLifecycleEvent.FEED_COLLECT_STARTED);
    }

    private void notifyActiveEventSubscribers(ActiveJobInfo jobInfo, ActiveLifecycleEvent event) {
        List<IActiveLifecycleEventSubscriber> subscribers = eventSubscribers.get(jobInfo.getActiveJobId());
        if (subscribers != null && !subscribers.isEmpty()) {
            for (IActiveLifecycleEventSubscriber subscriber : subscribers) {
                subscriber.handleEvent(event);
            }
        }

    }

    private void notifyFeedEventSubscribers(ActiveJobInfo jobInfo, ActiveLifecycleEvent event) {
        ActiveJopType jobType = jobInfo.getJobType();
        List<FeedConnectionId> impactedConnections = new ArrayList<FeedConnectionId>();
        if (jobType.equals(ActiveJopType.FEED_INTAKE)) {
            ActiveObjectId feedId = ((FeedIntakeInfo) jobInfo).getFeedId();
            for (ActiveJobId connId : eventSubscribers.keySet()) {
                if (connId.getActiveId().equals(feedId)) {
                    impactedConnections.add((FeedConnectionId) connId);
                }
            }
        } else {
            impactedConnections.add(((FeedConnectJobInfo) jobInfo).getConnectionId());
        }

        for (ActiveJobId connId : impactedConnections) {
            List<IActiveLifecycleEventSubscriber> subscribers = eventSubscribers.get(connId);
            if (subscribers != null && !subscribers.isEmpty()) {
                for (IActiveLifecycleEventSubscriber subscriber : subscribers) {
                    subscriber.handleEvent(event);
                }
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
                ActiveJobId connectionId = feedJoint.getProvider();
                FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
                locations = cInfo.getComputeLocations();
                break;
        }

        SubscribeFeedWork work = new SubscribeFeedWork(locations.toArray(new String[] {}), request);
        FeedWorkManager.INSTANCE.submitWork(work, new SubscribeFeedWork.FeedSubscribeWorkEventListener());
    }

    public IFeedJoint getSourceFeedJoint(ActiveJobId connectionId) {
        FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
        if (cInfo != null) {
            return cInfo.getSourceFeedJoint();
        }
        return null;
    }

    public Set<ActiveJobId> getActiveJobs() {
        Set<ActiveJobId> activeJobs = new HashSet<ActiveJobId>();
        for (ActiveJobInfo cInfo : jobInfos.values()) {
            if (cInfo.getState().equals(JobState.ACTIVE)) {
                activeJobs.add(cInfo.getActiveJobId());
            }
        }
        return activeJobs;
    }

    public Set<FeedConnectionId> getActiveFeedConnections() {
        Set<FeedConnectionId> activeConnections = new HashSet<FeedConnectionId>();
        for (FeedConnectJobInfo cInfo : connectJobInfos.values()) {
            if (cInfo.getState().equals(JobState.ACTIVE)) {
                activeConnections.add(cInfo.getConnectionId());
            }
        }
        return activeConnections;
    }

    public boolean isFeedConnectionActive(ActiveJobId connectionId) {
        FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
        if (cInfo != null) {
            return cInfo.getState().equals(JobState.ACTIVE);
        }
        return false;
    }

    public boolean isActive(ActiveJobId activeJobId) {
        ActiveJobInfo cInfo = activeJobInfos.get(activeJobId);
        if (cInfo != null) {
            return cInfo.getState().equals(JobState.ACTIVE);
        }
        return false;
    }

    public void setJobState(ActiveJobId activeJobId, JobState jobState) {
        ActiveJobInfo jobInfo = activeJobInfos.get(activeJobId);
        jobInfo.setState(jobState);
    }

    public JobState getJobState(ActiveJobId activeJobId) {
        return activeJobInfos.get(activeJobId).getState();
    }

    private void handleActiveJobFinishMessage(ActiveJobInfo jobInfo, Message message) throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(message.jobId);

        deregisterActiveJob(message.jobId);

        // notify event listeners 
        JobStatus status = info.getStatus();
        ActiveLifecycleEvent event;
        event = status.equals(JobStatus.FAILURE) ? ActiveLifecycleEvent.ACTIVE_JOB_FAILURE
                : ActiveLifecycleEvent.ACTIVE_JOB_ENDED;
        notifyActiveEventSubscribers(jobInfo, event);

    }

    private void handleFeedIntakeJobFinishMessage(FeedIntakeInfo intakeInfo, Message message) throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(message.jobId);
        JobStatus status = info.getStatus();
        ActiveLifecycleEvent event;
        event = status.equals(JobStatus.FAILURE) ? ActiveLifecycleEvent.FEED_INTAKE_FAILURE
                : ActiveLifecycleEvent.FEED_ENDED;

        // remove feed joints
        deregisterFeedIntakeJob(message.jobId);

        // notify event listeners 
        notifyFeedEventSubscribers(intakeInfo, event);

    }

    private void handleFeedCollectJobFinishMessage(FeedConnectJobInfo cInfo) throws Exception {
        ActiveJobId connectionId = cInfo.getConnectionId();

        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(cInfo.getJobId());
        JobStatus status = info.getStatus();
        boolean failure = status != null && status.equals(JobStatus.FAILURE);
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(cInfo.getFeedPolicy());

        boolean removeJobHistory = !failure;
        boolean retainSubsription = cInfo.getState().equals(JobState.UNDER_RECOVERY)
                || (failure && fpa.continueOnHardwareFailure());

        if (!retainSubsription) {
            IFeedJoint feedJoint = cInfo.getSourceFeedJoint();
            feedJoint.removeReceiver(connectionId);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Subscription " + cInfo.getConnectionId() + " completed successfully. Removed subscription");
            }
            removeFeedJointsPostPipelineTermination(cInfo.getConnectionId());
        }

        if (removeJobHistory) {
            connectJobInfos.remove(connectionId);
            jobInfos.remove(cInfo.getJobId());
            feedIntakeProgressTrackers.remove(cInfo.getConnectionId());
        }
        deregisterActivity(cInfo);

        // notify event listeners 
        ActiveLifecycleEvent event = failure ? ActiveLifecycleEvent.FEED_COLLECT_FAILURE
                : ActiveLifecycleEvent.FEED_ENDED;
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
            FeedActivity feedActivity = new FeedActivity(cInfo.getConnectionId().getDataverse(), cInfo
                    .getConnectionId().getName(), cInfo.getConnectionId().getDatasetName(),
                    feedActivityDetails);
            CentralActiveManager.getInstance().getLoadManager()
                    .reportActivity(cInfo.getConnectionId(), feedActivity);

        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to register feed activity for " + cInfo + " " + e.getMessage());
            }

        }

    }

    private void registerActivity(ChannelJobInfo cInfo) {
        //TODO: REGISTER/DEREGISTER OTHER ACTIVE JOBS
        Map<String, String> channelActivityDetails = new HashMap<String, String>();

        if (cInfo.getLocation() != null) {
            channelActivityDetails.put(ChannelActivity.ChannelActivityDetails.CHANNEL_LOCATIONS,
                    StringUtils.join(cInfo.getLocation().iterator(), ','));
        }

        channelActivityDetails.put(ChannelActivity.ChannelActivityDetails.CHANNEL_TIMESTAMP, (new Date()).toString());
        try {
            ChannelActivity channelActivity = new ChannelActivity(cInfo.getActiveJobId().getDataverse(),
                    cInfo.getActiveJobId().getName(), channelActivityDetails);
            CentralActiveManager.getInstance().getLoadManager()
                    .reportActivity(cInfo.getActiveJobId(), channelActivity);

        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to register activity for " + cInfo + " " + e.getMessage());
            }

        }

    }

    public void deregisterActivity(ActiveJobInfo cInfo) {
        try {
            CentralActiveManager.getInstance().getLoadManager().removeActivity(cInfo.getActiveJobId());
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to deregister activity for " + cInfo + " " + e.getMessage());
            }
        }
    }

    public void removeFeedJointsPostPipelineTermination(ActiveJobId connectionId) {
        FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
        List<IFeedJoint> feedJoints = feedPipeline.get(connectionId.getActiveId());

        IFeedJoint sourceJoint = cInfo.getSourceFeedJoint();
        List<FeedConnectionId> all = sourceJoint.getReceivers();
        boolean removeSourceJoint = all.size() < 2;
        if (removeSourceJoint) {
            feedJoints.remove(sourceJoint);
        }

        IFeedJoint computeJoint = cInfo.getComputeFeedJoint();
        if (computeJoint != null && computeJoint.getReceivers().size() < 2) {
            feedJoints.remove(computeJoint);
        }
    }

    public boolean isRegisteredJob(JobId jobId) {
        return jobInfos.get(jobId) != null;
    }

    public List<String> getJobLocations(ActiveJobId activeJobId) {
        //TODO: This will be changed when channels become procedures
        return ((ChannelJobInfo) activeJobInfos.get(activeJobId)).getLocation();
    }

    public List<String> getFeedComputeLocations(ActiveObjectId feedId) {
        List<IFeedJoint> feedJoints = feedPipeline.get(feedId);
        for (IFeedJoint joint : feedJoints) {
            if (joint.getFeedJointKey().getFeedId().equals(feedId)) {
                return connectJobInfos.get(joint.getProvider()).getComputeLocations();
            }
        }
        return null;
    }

    public List<String> getFeedStorageLocations(ActiveJobId connectionId) {
        return connectJobInfos.get(connectionId).getStorageLocations();
    }

    public List<String> getFeedCollectLocations(ActiveJobId connectionId) {
        return connectJobInfos.get(connectionId).getCollectLocations();
    }

    public List<String> getFeedIntakeLocations(ActiveObjectId feedId) {
        return intakeJobInfos.get(feedId).getIntakeLocation();
    }

    public JobId getFeedCollectJobId(ActiveJobId connectionId) {
        return connectJobInfos.get(connectionId).getJobId();
    }

    public void registerEventSubscriber(ActiveJobId activeJobId, IActiveLifecycleEventSubscriber subscriber) {
        List<IActiveLifecycleEventSubscriber> subscribers = eventSubscribers.get(activeJobId);
        if (subscribers == null) {
            subscribers = new ArrayList<IActiveLifecycleEventSubscriber>();
            eventSubscribers.put(activeJobId, subscribers);
        }
        subscribers.add(subscriber);
    }

    public void deregisterEventSubscriber(ActiveJobId activeJobId, IActiveLifecycleEventSubscriber subscriber) {
        List<IActiveLifecycleEventSubscriber> subscribers = eventSubscribers.get(activeJobId);
        if (subscribers != null) {
            subscribers.remove(subscriber);
        }
    }

    //============================

    public boolean isFeedPointAvailable(FeedJointKey feedJointKey) {
        List<IFeedJoint> joints = feedPipeline.get(feedJointKey.getFeedId());
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
        List<IFeedJoint> joints = feedPipeline.get(feedPointKey.getFeedId());
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
            List<IFeedJoint> jointsOnPipeline = feedPipeline.get(feedJointKey.getFeedId());
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

    public ActiveJobInfo getActiveJobInfo(ActiveJobId activeJobId) {
        return activeJobInfos.get(activeJobId);
    }

    //Is this right???? called collect but gets from connect
    public JobSpecification getCollectJobSpecification(ActiveJobId connectionId) {
        return connectJobInfos.get(connectionId).getSpec();
    }

    public JobSpecification getActiveJobSpecification(ActiveObjectId activeId) {
        return jobInfos.get(activeId).getSpec();
    }

    public IFeedJoint getFeedPoint(ActiveObjectId sourceFeedId, IFeedJoint.FeedJointType type) {
        List<IFeedJoint> joints = feedPipeline.get(sourceFeedId);
        for (IFeedJoint joint : joints) {
            if (joint.getType().equals(type)) {
                return joint;
            }
        }
        return null;
    }

    public FeedConnectJobInfo getFeedConnectJobInfo(ActiveJobId connectionId) {
        return connectJobInfos.get(connectionId);
    }

    //TODO: Set locations for Procedures
    private void setLocations(ChannelJobInfo cInfo) {
        JobSpecification jobSpec = cInfo.getSpec();

        List<OperatorDescriptorId> OperatorIds = new ArrayList<OperatorDescriptorId>();

        Map<OperatorDescriptorId, IOperatorDescriptor> operators = jobSpec.getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            IOperatorDescriptor actualOp = null;
            if (opDesc instanceof ChannelMetaOperatorDescriptor) {
                actualOp = ((ChannelMetaOperatorDescriptor) opDesc).getCoreOperator();
            } else {
                actualOp = opDesc;
            }

            if (actualOp instanceof RepetitiveChannelOperatorDescriptor) {
                OperatorIds.add(entry.getKey());
            }
        }

        try {
            IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
            JobInfo info = hcc.getJobInfo(cInfo.getJobId());
            List<String> locations = new ArrayList<String>();
            for (OperatorDescriptorId opId : OperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(opId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    locations.add(operatorLocations.get(i));
                }
            }

            cInfo.setLocation(locations);

        } catch (Exception e) {
            e.printStackTrace();
        }

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