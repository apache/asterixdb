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
package edu.uci.ics.asterix.feeds;

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

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.asterix.api.common.FeedWorkCollection.SubscribeFeedWork;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.feeds.FeedActivity;
import edu.uci.ics.asterix.common.feeds.FeedConnectJobInfo;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedConnectionRequest;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedIntakeInfo;
import edu.uci.ics.asterix.common.feeds.FeedJobInfo;
import edu.uci.ics.asterix.common.feeds.FeedJobInfo.FeedJobState;
import edu.uci.ics.asterix.common.feeds.FeedJobInfo.JobType;
import edu.uci.ics.asterix.common.feeds.FeedJointKey;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.common.feeds.api.IFeedJoint;
import edu.uci.ics.asterix.common.feeds.api.IFeedJoint.State;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleEventSubscriber;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleEventSubscriber.FeedLifecycleEvent;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;
import edu.uci.ics.asterix.common.feeds.message.StorageReportFeedMessage;
import edu.uci.ics.asterix.feeds.FeedLifecycleListener.Message;
import edu.uci.ics.asterix.metadata.feeds.BuiltinFeedPolicies;
import edu.uci.ics.asterix.metadata.feeds.FeedCollectOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedMetaOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedWorkManager;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobInfo;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;

public class FeedJobNotificationHandler implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(FeedJobNotificationHandler.class.getName());

    private final LinkedBlockingQueue<Message> inbox;
    private final Map<FeedConnectionId, List<IFeedLifecycleEventSubscriber>> eventSubscribers;

    private final Map<JobId, FeedJobInfo> jobInfos;
    private final Map<FeedId, FeedIntakeInfo> intakeJobInfos;
    private final Map<FeedConnectionId, FeedConnectJobInfo> connectJobInfos;
    private final Map<FeedId, List<IFeedJoint>> feedPipeline;
    private final Map<FeedConnectionId, Pair<IIntakeProgressTracker, Long>> feedIntakeProgressTrackers;

    public FeedJobNotificationHandler(LinkedBlockingQueue<Message> inbox) {
        this.inbox = inbox;
        this.jobInfos = new HashMap<JobId, FeedJobInfo>();
        this.intakeJobInfos = new HashMap<FeedId, FeedIntakeInfo>();
        this.connectJobInfos = new HashMap<FeedConnectionId, FeedConnectJobInfo>();
        this.feedPipeline = new HashMap<FeedId, List<IFeedJoint>>();
        this.eventSubscribers = new HashMap<FeedConnectionId, List<IFeedLifecycleEventSubscriber>>();
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

    public void registerFeedIntakeJob(FeedId feedId, JobId jobId, JobSpecification jobSpec) throws HyracksDataException {
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
            FeedIntakeInfo intakeJobInfo = new FeedIntakeInfo(jobId, FeedJobState.CREATED, FeedJobInfo.JobType.INTAKE,
                    feedId, intakeJoint, jobSpec);
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

    public void registerFeedCollectionJob(FeedId sourceFeedId, FeedConnectionId connectionId, JobId jobId,
            JobSpecification jobSpec, Map<String, String> feedPolicy) {
        if (jobInfos.get(jobId) != null) {
            throw new IllegalStateException("Feed job already registered");
        }

        List<IFeedJoint> feedJoints = feedPipeline.get(sourceFeedId);
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

    public void deregisterFeedIntakeJob(JobId jobId) {
        if (jobInfos.get(jobId) == null) {
            throw new IllegalStateException(" Feed Intake job not registered ");
        }

        FeedIntakeInfo info = (FeedIntakeInfo) jobInfos.get(jobId);
        jobInfos.remove(jobId);
        intakeJobInfos.remove(info.getFeedId());

        if (!info.getState().equals(FeedJobState.UNDER_RECOVERY)) {
            List<IFeedJoint> joints = feedPipeline.get(info.getFeedId());
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

    private void handleJobStartMessage(Message message) throws Exception {
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

    private void handleJobFinishMessage(Message message) throws Exception {
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
                    LOGGER.info("Collect Job finished for  " + (FeedConnectJobInfo) jobInfo);
                }
                handleFeedCollectJobFinishMessage((FeedConnectJobInfo) jobInfo);
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
        intakeJobInfo.getIntakeFeedJoint().setState(State.ACTIVE);
        intakeJobInfo.setState(FeedJobState.ACTIVE);

        // notify event listeners 
        notifyFeedEventSubscribers(intakeJobInfo, FeedLifecycleEvent.FEED_INTAKE_STARTED);
    }

    private void handleCollectJobStartMessage(FeedConnectJobInfo cInfo) throws RemoteException, ACIDException {
        // set locations of feed sub-operations (intake, compute, store)
        setLocations(cInfo);

        // activate joints
        List<IFeedJoint> joints = feedPipeline.get(cInfo.getConnectionId().getFeedId());
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
        // notify event listeners
        notifyFeedEventSubscribers(cInfo, FeedLifecycleEvent.FEED_COLLECT_STARTED);
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

    public boolean isFeedConnectionActive(FeedConnectionId connectionId) {
        FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
        if (cInfo != null) {
            return cInfo.getState().equals(FeedJobState.ACTIVE);
        }
        return false;
    }

    public void setJobState(FeedConnectionId connectionId, FeedJobState jobState) {
        FeedConnectJobInfo connectJobInfo = connectJobInfos.get(connectionId);
        connectJobInfo.setState(jobState);
    }

    public FeedJobState getFeedJobState(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getState();
    }

    private void handleFeedIntakeJobFinishMessage(FeedIntakeInfo intakeInfo, Message message) throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(message.jobId);
        JobStatus status = info.getStatus();
        FeedLifecycleEvent event;
        event = status.equals(JobStatus.FAILURE) ? FeedLifecycleEvent.FEED_INTAKE_FAILURE
                : FeedLifecycleEvent.FEED_ENDED;

        // remove feed joints
        deregisterFeedIntakeJob(message.jobId);

        // notify event listeners 
        notifyFeedEventSubscribers(intakeInfo, event);

    }

    private void handleFeedCollectJobFinishMessage(FeedConnectJobInfo cInfo) throws Exception {
        FeedConnectionId connectionId = cInfo.getConnectionId();

        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(cInfo.getJobId());
        JobStatus status = info.getStatus();
        boolean failure = status != null && status.equals(JobStatus.FAILURE);
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(cInfo.getFeedPolicy());

        boolean removeJobHistory = !failure;
        boolean retainSubsription = cInfo.getState().equals(FeedJobState.UNDER_RECOVERY)
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
        deregisterFeedActivity(cInfo);

        // notify event listeners 
        FeedLifecycleEvent event = failure ? FeedLifecycleEvent.FEED_COLLECT_FAILURE : FeedLifecycleEvent.FEED_ENDED;
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
            FeedActivity feedActivity = new FeedActivity(cInfo.getConnectionId().getFeedId().getDataverse(), cInfo
                    .getConnectionId().getFeedId().getFeedName(), cInfo.getConnectionId().getDatasetName(),
                    feedActivityDetails);
            CentralFeedManager.getInstance().getFeedLoadManager()
                    .reportFeedActivity(cInfo.getConnectionId(), feedActivity);

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

    public void removeFeedJointsPostPipelineTermination(FeedConnectionId connectionId) {
        FeedConnectJobInfo cInfo = connectJobInfos.get(connectionId);
        List<IFeedJoint> feedJoints = feedPipeline.get(connectionId.getFeedId());

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

    public boolean isRegisteredFeedJob(JobId jobId) {
        return jobInfos.get(jobId) != null;
    }

    public List<String> getFeedComputeLocations(FeedId feedId) {
        List<IFeedJoint> feedJoints = feedPipeline.get(feedId);
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

    public void registerFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber) {
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

    public JobSpecification getCollectJobSpecification(FeedConnectionId connectionId) {
        return connectJobInfos.get(connectionId).getSpec();
    }

    public IFeedJoint getFeedPoint(FeedId sourceFeedId, IFeedJoint.FeedJointType type) {
        List<IFeedJoint> joints = feedPipeline.get(sourceFeedId);
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