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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.api.common.SessionConfig;
import org.apache.asterix.api.common.SessionConfig.OutputFormat;
import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.DataverseDecl;
import org.apache.asterix.aql.expression.DisconnectFeedStatement;
import org.apache.asterix.aql.expression.Identifier;
import org.apache.asterix.aql.translator.AqlTranslator;
import org.apache.asterix.common.active.ActiveId;
import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.ActiveJobInfo;
import org.apache.asterix.common.active.ActiveJobInfo.ActiveJopType;
import org.apache.asterix.common.active.ActiveJobInfo.JobState;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.IClusterManagementWorkResponse;
import org.apache.asterix.common.channels.ChannelJobInfo;
import org.apache.asterix.common.feeds.FeedConnectJobInfo;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConnectionRequest;
import org.apache.asterix.common.feeds.FeedIntakeInfo;
import org.apache.asterix.common.feeds.FeedJointKey;
import org.apache.asterix.common.feeds.api.IActiveLifecycleEventSubscriber;
import org.apache.asterix.common.feeds.api.IFeedJoint;
import org.apache.asterix.common.feeds.api.IFeedLifecycleListener;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.common.feeds.message.StorageReportFeedMessage;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.channels.ChannelMetaOperatorDescriptor;
import org.apache.asterix.metadata.channels.RepetitiveChannelOperatorDescriptor;
import org.apache.asterix.metadata.cluster.AddNodeWork;
import org.apache.asterix.metadata.cluster.ClusterManager;
import org.apache.asterix.metadata.feeds.FeedCollectOperatorDescriptor;
import org.apache.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

/**
 * A listener that subscribes to events associated with cluster membership
 * (nodes joining/leaving the cluster) and job lifecycle (start/end of a job).
 * Subscription to such events allows keeping track of feed ingestion jobs and
 * take any corrective action that may be required when a node involved in a
 * feed leaves the cluster.
 */
public class ActiveJobLifecycleListener implements IFeedLifecycleListener {

    private static final Logger LOGGER = Logger.getLogger(ActiveJobLifecycleListener.class.getName());

    public static ActiveJobLifecycleListener INSTANCE = new ActiveJobLifecycleListener();

    private final LinkedBlockingQueue<Message> jobEventInbox;
    private final LinkedBlockingQueue<IClusterManagementWorkResponse> responseInbox;
    private final Map<FeedCollectInfo, List<String>> dependentFeeds = new HashMap<FeedCollectInfo, List<String>>();
    private final Map<FeedConnectionId, LinkedBlockingQueue<String>> feedReportQueue;
    private final ActiveJobNotificationHandler jobNotificationHandler;
    private final ActiveWorkRequestResponseHandler workRequestResponseHandler;
    private final ExecutorService executorService;

    private ClusterState state;

    private ActiveJobLifecycleListener() {
        this.jobEventInbox = new LinkedBlockingQueue<Message>();
        this.jobNotificationHandler = new ActiveJobNotificationHandler(jobEventInbox);
        this.responseInbox = new LinkedBlockingQueue<IClusterManagementWorkResponse>();
        this.workRequestResponseHandler = new ActiveWorkRequestResponseHandler(responseInbox);
        this.feedReportQueue = new HashMap<FeedConnectionId, LinkedBlockingQueue<String>>();
        this.executorService = Executors.newCachedThreadPool();
        this.executorService.execute(jobNotificationHandler);
        this.executorService.execute(workRequestResponseHandler);
        ClusterManager.INSTANCE.registerSubscriber(this);
        this.state = AsterixClusterProperties.INSTANCE.getState();
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {
        if (jobNotificationHandler.isRegisteredJob(jobId)) {
            jobEventInbox.add(new Message(jobId, Message.MessageKind.JOB_START));
        }
    }

    @Override
    public void notifyJobFinish(JobId jobId) throws HyracksException {
        if (jobNotificationHandler.isRegisteredJob(jobId)) {
            jobEventInbox.add(new Message(jobId, Message.MessageKind.JOB_FINISH));
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("NO NEED TO NOTIFY JOB FINISH!");
            }
        }
    }

    public FeedConnectJobInfo getFeedConnectJobInfo(ActiveJobId connectionId) {
        return jobNotificationHandler.getFeedConnectJobInfo(connectionId);
    }

    public ActiveJobInfo getActiveJobInfo(ActiveJobId activeJobId) {
        return jobNotificationHandler.getActiveJobInfo(activeJobId);
    }

    public void registerFeedIntakeProgressTracker(FeedConnectionId connectionId,
            IIntakeProgressTracker feedIntakeProgressTracker) {
        jobNotificationHandler.registerFeedIntakeProgressTracker(connectionId, feedIntakeProgressTracker);
    }

    public void deregisterFeedIntakeProgressTracker(ActiveJobId connectionId) {
        jobNotificationHandler.deregisterFeedIntakeProgressTracker(connectionId);
    }

    public void updateTrackingInformation(StorageReportFeedMessage srm) {
        jobNotificationHandler.updateTrackingInformation(srm);
    }

    /*
     * Traverse job specification to categorize job 
     */
    @Override
    public void notifyJobCreation(JobId jobId, IActivityClusterGraphGeneratorFactory acggf) throws HyracksException {
        JobSpecification spec = acggf.getJobSpecification();
        FeedConnectionId feedConnectionId = null;
        Map<String, String> feedPolicy = null;
        for (IOperatorDescriptor opDesc : spec.getOperatorMap().values()) {
            if (opDesc instanceof FeedCollectOperatorDescriptor) {
                feedConnectionId = ((FeedCollectOperatorDescriptor) opDesc).getFeedConnectionId();
                feedPolicy = ((FeedCollectOperatorDescriptor) opDesc).getFeedPolicyProperties();
                jobNotificationHandler.registerFeedCollectionJob(
                        ((FeedCollectOperatorDescriptor) opDesc).getSourceFeedId(), feedConnectionId, jobId, spec,
                        feedPolicy);
                break;
            } else if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                jobNotificationHandler.registerFeedIntakeJob(((FeedIntakeOperatorDescriptor) opDesc).getFeedId(),
                        jobId, spec);
                break;
            } else if (opDesc instanceof ChannelMetaOperatorDescriptor) {
                ActiveJobId channelJobId = null;
                RepetitiveChannelOperatorDescriptor channelOp = (RepetitiveChannelOperatorDescriptor) ((ChannelMetaOperatorDescriptor) opDesc)
                        .getCoreOperator();
                channelJobId = (channelOp).getChannelJobId();
                jobNotificationHandler.registerActiveJob(channelJobId, jobId, ActiveJopType.CHANNEL_REPETITIVE, spec);
                break;
            }
        }
    }

    public void setJobState(ActiveJobId connectionId, JobState jobState) {
        jobNotificationHandler.setJobState(connectionId, jobState);
    }

    public JobState getJobState(ActiveJobId connectionId) {
        return jobNotificationHandler.getJobState(connectionId);
    }

    public static class Message {
        public JobId jobId;

        public enum MessageKind {
            JOB_START,
            JOB_FINISH
        }

        public MessageKind messageKind;

        public Message(JobId jobId, MessageKind msgKind) {
            this.jobId = jobId;
            this.messageKind = msgKind;
        }
    }

    @Override
    public Set<IClusterManagementWork> notifyNodeFailure(Set<String> deadNodeIds) {
        Set<IClusterManagementWork> workToBeDone = new HashSet<IClusterManagementWork>();

        Collection<FeedIntakeInfo> intakeInfos = jobNotificationHandler.getFeedIntakeInfos();
        Collection<FeedConnectJobInfo> connectJobInfos = jobNotificationHandler.getFeedConnectInfos();
        Collection<ActiveJobInfo> activeJobInfos = jobNotificationHandler.getActiveJobInfos();

        Map<String, List<ActiveJobInfo>> impactedJobs = new HashMap<String, List<ActiveJobInfo>>();

        for (String deadNode : deadNodeIds) {
            for (FeedIntakeInfo intakeInfo : intakeInfos) {
                if (intakeInfo.getIntakeLocation().contains(deadNode)) {
                    List<ActiveJobInfo> infos = impactedJobs.get(deadNode);
                    if (infos == null) {
                        infos = new ArrayList<ActiveJobInfo>();
                        impactedJobs.put(deadNode, infos);
                    }
                    infos.add(intakeInfo);
                    intakeInfo.setState(JobState.UNDER_RECOVERY);
                }
            }

            for (FeedConnectJobInfo jInfo : connectJobInfos) {
                if (jInfo instanceof FeedConnectJobInfo) {
                    FeedConnectJobInfo connectInfo = (FeedConnectJobInfo) jInfo;
                    if (connectInfo.getStorageLocations().contains(deadNode)) {
                        continue;
                    }
                    if (connectInfo.getComputeLocations().contains(deadNode)
                            || connectInfo.getCollectLocations().contains(deadNode)) {
                        List<ActiveJobInfo> infos = impactedJobs.get(deadNode);
                        if (infos == null) {
                            infos = new ArrayList<ActiveJobInfo>();
                            impactedJobs.put(deadNode, infos);
                        }
                        infos.add(connectInfo);
                        connectInfo.setState(JobState.UNDER_RECOVERY);
                        jobNotificationHandler.deregisterActivity(connectInfo);
                    }
                }
            }
            for (ActiveJobInfo jInfo : activeJobInfos) {
                //TODO:Handle other active jobs
                if (jInfo instanceof ChannelJobInfo) {
                    ChannelJobInfo cInfo = (ChannelJobInfo) jInfo;
                    if (cInfo.getLocation().contains(deadNode)) {
                        continue;
                    }
                    List<ActiveJobInfo> infos = impactedJobs.get(deadNode);
                    if (infos == null) {
                        infos = new ArrayList<ActiveJobInfo>();
                        impactedJobs.put(deadNode, infos);
                    }
                    infos.add(cInfo);
                    cInfo.setState(JobState.UNDER_RECOVERY);
                    jobNotificationHandler.deregisterActivity(cInfo);
                }

            }

        }

        if (impactedJobs.size() > 0) {
            AddNodeWork addNodeWork = new AddNodeWork(deadNodeIds, deadNodeIds.size(), this);
            workRequestResponseHandler.registerWork(addNodeWork.getWorkId(), impactedJobs);
            workToBeDone.add(addNodeWork);
        }
        return workToBeDone;

    }

    public static class FailureReport {
        //TODO: Account for otehr active jobs

        private final List<Pair<FeedConnectJobInfo, List<String>>> recoverableConnectJobs;
        private final Map<IFeedJoint, List<String>> recoverableIntakeFeedIds;

        public FailureReport(Map<IFeedJoint, List<String>> recoverableIntakeFeedIds,
                List<Pair<FeedConnectJobInfo, List<String>>> recoverableSubscribers) {
            this.recoverableConnectJobs = recoverableSubscribers;
            this.recoverableIntakeFeedIds = recoverableIntakeFeedIds;
        }

        public List<Pair<FeedConnectJobInfo, List<String>>> getRecoverableSubscribers() {
            return recoverableConnectJobs;
        }

        public Map<IFeedJoint, List<String>> getRecoverableIntakeFeedIds() {
            return recoverableIntakeFeedIds;
        }

    }

    //This doesn't actually work???
    //dependentfeeds is never set
    //Need to ask Raman about this
    @Override
    public Set<IClusterManagementWork> notifyNodeJoin(String joinedNodeId) {
        ClusterState newState = AsterixClusterProperties.INSTANCE.getState();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(joinedNodeId + " joined the cluster. " + "Asterix state: " + newState);
        }

        boolean needToReActivateFeeds = !newState.equals(state) && (newState == ClusterState.ACTIVE);
        if (needToReActivateFeeds) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(joinedNodeId + " Resuming loser feeds (if any)");
            }
            try {
                ActiveJobsActivator activator = new ActiveJobsActivator();
                (new Thread(activator)).start();
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Exception in resuming feeds" + e.getMessage());
                }
            }
            state = newState;
        } else {
            List<ActiveJobInfo> feedsThatCanBeRevived = new ArrayList<ActiveJobInfo>();
            for (Entry<FeedCollectInfo, List<String>> entry : dependentFeeds.entrySet()) {
                List<String> requiredNodeIds = entry.getValue();
                if (requiredNodeIds.contains(joinedNodeId)) {
                    requiredNodeIds.remove(joinedNodeId);
                    if (requiredNodeIds.isEmpty()) {
                        feedsThatCanBeRevived.add(entry.getKey());
                    }
                }
            }
            if (!feedsThatCanBeRevived.isEmpty()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(joinedNodeId + " Resuming feeds after rejoining of node " + joinedNodeId);
                }
                ActiveJobsActivator activator = new ActiveJobsActivator(feedsThatCanBeRevived);
                (new Thread(activator)).start();
            }
        }
        return null;
    }

    @Override
    public void notifyRequestCompletion(IClusterManagementWorkResponse response) {
        try {
            responseInbox.put(response);
        } catch (InterruptedException e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Interrupted exception");
            }
        }
    }

    @Override
    public void notifyStateChange(ClusterState previousState, ClusterState newState) {
        switch (newState) {
            case ACTIVE:
                if (previousState.equals(ClusterState.UNUSABLE)) {
                    try {
                        ActiveJobsActivator activator = new ActiveJobsActivator();
                        // (new Thread(activator)).start();
                    } catch (Exception e) {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Exception in resuming feeds" + e.getMessage());
                        }
                    }
                }
                break;
        }

    }

    //TODO: Deactivate other types of jobs
    public static class FeedsDeActivator implements Runnable {

        private List<FeedConnectJobInfo> failedConnectjobs;

        public FeedsDeActivator(List<FeedConnectJobInfo> failedConnectjobs) {
            this.failedConnectjobs = failedConnectjobs;
        }

        @Override
        public void run() {
            for (FeedConnectJobInfo failedConnectJob : failedConnectjobs) {
                endFeed(failedConnectJob);
            }
        }

        private void endFeed(FeedConnectJobInfo cInfo) {
            MetadataTransactionContext ctx = null;
            PrintWriter writer = new PrintWriter(System.out, true);
            SessionConfig pc = new SessionConfig(writer, OutputFormat.ADM);

            try {
                ctx = MetadataManager.INSTANCE.beginTransaction();
                ActiveId feedId = cInfo.getConnectionId().getActiveId();
                DisconnectFeedStatement stmt = new DisconnectFeedStatement(new Identifier(feedId.getDataverse()),
                        new Identifier(feedId.getName()), new Identifier(cInfo.getConnectionId().getDatasetName()));
                List<Statement> statements = new ArrayList<Statement>();
                DataverseDecl dataverseDecl = new DataverseDecl(new Identifier(feedId.getDataverse()));
                statements.add(dataverseDecl);
                statements.add(stmt);
                AqlTranslator translator = new AqlTranslator(statements, pc);
                translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null,
                        AqlTranslator.ResultDelivery.SYNC);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("End irrecoverable feed: " + cInfo.getConnectionId());
                }
                MetadataManager.INSTANCE.commitTransaction(ctx);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Exception in ending loser feed: " + cInfo.getConnectionId() + " Exception "
                            + e.getMessage());
                }
                e.printStackTrace();
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (Exception e2) {
                    e2.addSuppressed(e);
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Exception in aborting transaction! System is in inconsistent state");
                    }
                }

            }

        }
    }

    public void submitFeedConnectionRequest(IFeedJoint feedPoint, FeedConnectionRequest subscriptionRequest)
            throws Exception {
        jobNotificationHandler.submitFeedConnectionRequest(feedPoint, subscriptionRequest);
    }

    @Override
    public List<FeedConnectionId> getActiveFeedConnections(ActiveId feedId) {
        List<FeedConnectionId> connections = new ArrayList<FeedConnectionId>();
        Collection<FeedConnectionId> activeConnections = jobNotificationHandler.getActiveFeedConnections();
        if (feedId != null) {
            for (FeedConnectionId connectionId : activeConnections) {
                if (connectionId.getActiveId().equals(feedId)) {
                    connections.add(connectionId);
                }
            }
        } else {
            connections.addAll(activeConnections);
        }
        return connections;
    }

    @Override
    public List<String> getComputeLocations(ActiveId feedId) {
        return jobNotificationHandler.getFeedComputeLocations(feedId);
    }

    @Override
    public List<String> getIntakeLocations(ActiveId feedId) {
        return jobNotificationHandler.getFeedIntakeLocations(feedId);
    }

    @Override
    public List<String> getStoreLocations(ActiveJobId feedConnectionId) {
        return jobNotificationHandler.getFeedStorageLocations(feedConnectionId);
    }

    @Override
    public List<String> getCollectLocations(ActiveJobId feedConnectionId) {
        return jobNotificationHandler.getFeedCollectLocations(feedConnectionId);
    }

    @Override
    public boolean isFeedConnectionActive(ActiveJobId connectionId) {
        return jobNotificationHandler.isFeedConnectionActive(connectionId);
    }

    public void reportPartialDisconnection(ActiveJobId connectionId) {
        jobNotificationHandler.removeFeedJointsPostPipelineTermination(connectionId);
    }

    public void registerFeedReportQueue(FeedConnectionId feedId, LinkedBlockingQueue<String> queue) {
        feedReportQueue.put(feedId, queue);
    }

    public void deregisterFeedReportQueue(ActiveJobId feedId, LinkedBlockingQueue<String> queue) {
        feedReportQueue.remove(feedId);
    }

    public LinkedBlockingQueue<String> getFeedReportQueue(ActiveJobId feedId) {
        return feedReportQueue.get(feedId);
    }

    @Override
    public IFeedJoint getAvailableFeedJoint(FeedJointKey feedJointKey) {
        return jobNotificationHandler.getAvailableFeedJoint(feedJointKey);
    }

    @Override
    public boolean isFeedJointAvailable(FeedJointKey feedJointKey) {
        return jobNotificationHandler.isFeedPointAvailable(feedJointKey);
    }

    public boolean isActive(ActiveJobId activeJobId) {
        return jobNotificationHandler.isActive(activeJobId);
    }

    public List<String> getLocations(ActiveJobId activeJobId) {
        return jobNotificationHandler.getJobLocations(activeJobId);
    }

    public void registerFeedJoint(IFeedJoint feedJoint) {
        jobNotificationHandler.registerFeedJoint(feedJoint);
    }

    public IFeedJoint getFeedJoint(FeedJointKey feedJointKey) {
        return jobNotificationHandler.getFeedJoint(feedJointKey);
    }

    public void registerFeedEventSubscriber(FeedConnectionId connectionId, IActiveLifecycleEventSubscriber subscriber) {
        jobNotificationHandler.registerEventSubscriber(connectionId, subscriber);
    }

    public void deregisterFeedEventSubscriber(ActiveJobId connectionId, IActiveLifecycleEventSubscriber subscriber) {
        jobNotificationHandler.deregisterEventSubscriber(connectionId, subscriber);

    }

    public JobSpecification getCollectJobSpecification(ActiveJobId connectionId) {
        return jobNotificationHandler.getCollectJobSpecification(connectionId);
    }

    public JobId getFeedCollectJobId(ActiveJobId connectionId) {
        return jobNotificationHandler.getFeedCollectJobId(connectionId);
    }

}
