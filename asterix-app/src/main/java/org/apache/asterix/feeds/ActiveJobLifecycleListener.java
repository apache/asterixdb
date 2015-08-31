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
package org.apache.asterix.feeds;

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

import org.apache.asterix.channels.ChannelNotificationHandler;
import org.apache.asterix.common.active.ActiveJobInfo;
import org.apache.asterix.common.active.ActiveJobInfo.JobState;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.IClusterManagementWorkResponse;
import org.apache.asterix.common.channels.ChannelId;
import org.apache.asterix.common.channels.ChannelJobInfo;
import org.apache.asterix.common.channels.api.IChannelLifecycleEventSubscriber;
import org.apache.asterix.common.feeds.FeedConnectJobInfo;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConnectionRequest;
import org.apache.asterix.common.feeds.FeedId;
import org.apache.asterix.common.feeds.FeedIntakeInfo;
import org.apache.asterix.common.feeds.FeedJointKey;
import org.apache.asterix.common.feeds.api.IActiveJobLifeCycleListener;
import org.apache.asterix.common.feeds.api.IFeedJoint;
import org.apache.asterix.common.feeds.api.IFeedLifecycleEventSubscriber;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.common.feeds.message.StorageReportFeedMessage;
import org.apache.asterix.metadata.channels.ChannelMetaOperatorDescriptor;
import org.apache.asterix.metadata.channels.RepetitiveChannelOperatorDescriptor;
import org.apache.asterix.metadata.cluster.AddNodeWork;
import org.apache.asterix.metadata.cluster.ClusterManager;
import org.apache.asterix.metadata.feeds.FeedCollectOperatorDescriptor;
import org.apache.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
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
 * Subscription to such events allows keeping track of feed/channel jobs and
 * take any corrective action that may be required when a node involved in an
 * active job leaves the cluster.
 */
public class ActiveJobLifecycleListener extends IActiveJobLifeCycleListener {

    private static final Logger LOGGER = Logger.getLogger(ActiveJobLifecycleListener.class.getName());

    public static ActiveJobLifecycleListener INSTANCE = new ActiveJobLifecycleListener();

    private final LinkedBlockingQueue<Message> feedEventInbox;
    private final LinkedBlockingQueue<Message> channelEventInbox;
    private final LinkedBlockingQueue<IClusterManagementWorkResponse> responseInbox;
    private final Map<ActiveJobInfo, List<String>> dependentFeeds = new HashMap<ActiveJobInfo, List<String>>();
    private final Map<FeedConnectionId, LinkedBlockingQueue<String>> feedReportQueue;
    private final Map<ChannelId, LinkedBlockingQueue<String>> channelReportQueue;
    private final FeedJobNotificationHandler feedJobNotificationHandler;
    private final ChannelNotificationHandler channelNotificationHandler;
    private final ActiveWorkRequestResponseHandler activeWorkRequestResponseHandler;
    private final ExecutorService executorService;

    private ClusterState state;

    private ActiveJobLifecycleListener() {
        this.feedEventInbox = new LinkedBlockingQueue<Message>();
        this.channelEventInbox = new LinkedBlockingQueue<Message>();
        this.feedJobNotificationHandler = new FeedJobNotificationHandler(feedEventInbox);
        this.channelNotificationHandler = new ChannelNotificationHandler(channelEventInbox);
        this.responseInbox = new LinkedBlockingQueue<IClusterManagementWorkResponse>();
        this.activeWorkRequestResponseHandler = new ActiveWorkRequestResponseHandler(responseInbox);
        this.feedReportQueue = new HashMap<FeedConnectionId, LinkedBlockingQueue<String>>();
        this.channelReportQueue = new HashMap<ChannelId, LinkedBlockingQueue<String>>();
        this.executorService = Executors.newCachedThreadPool();
        this.executorService.execute(feedJobNotificationHandler);
        this.executorService.execute(channelNotificationHandler);
        this.executorService.execute(activeWorkRequestResponseHandler);
        ClusterManager.INSTANCE.registerSubscriber(this);
        this.state = AsterixClusterProperties.INSTANCE.getState();
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {
        if (feedJobNotificationHandler.isRegisteredFeedJob(jobId)) {
            feedEventInbox.add(new Message(jobId, Message.MessageKind.JOB_START));
        } else if (channelNotificationHandler.isRegisteredChannelJob(jobId)) {
            channelEventInbox.add(new Message(jobId, Message.MessageKind.JOB_START));
        }
    }

    @Override
    public void notifyJobFinish(JobId jobId) throws HyracksException {
        if (feedJobNotificationHandler.isRegisteredFeedJob(jobId)) {
            feedEventInbox.add(new Message(jobId, Message.MessageKind.JOB_FINISH));
        } else if (channelNotificationHandler.isRegisteredChannelJob(jobId)) {
            channelEventInbox.add(new Message(jobId, Message.MessageKind.JOB_FINISH));

        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("NO NEED TO NOTIFY JOB FINISH!");
            }
        }
    }

    public FeedConnectJobInfo getFeedConnectJobInfo(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getFeedConnectJobInfo(connectionId);
    }

    public void registerFeedIntakeProgressTracker(FeedConnectionId connectionId,
            IIntakeProgressTracker feedIntakeProgressTracker) {
        feedJobNotificationHandler.registerFeedIntakeProgressTracker(connectionId, feedIntakeProgressTracker);
    }

    public void deregisterFeedIntakeProgressTracker(FeedConnectionId connectionId) {
        feedJobNotificationHandler.deregisterFeedIntakeProgressTracker(connectionId);
    }

    public void updateTrackingInformation(StorageReportFeedMessage srm) {
        feedJobNotificationHandler.updateTrackingInformation(srm);
    }

    public ChannelJobInfo getChannelJobInfo(ChannelId channelId) {
        return channelNotificationHandler.getChannelJobInfo(channelId);
    }

    /*
     * Traverse job specification to categorize job as a feed intake job or a feed collection job 
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
                feedJobNotificationHandler.registerFeedCollectionJob(
                        ((FeedCollectOperatorDescriptor) opDesc).getSourceFeedId(), feedConnectionId, jobId, spec,
                        feedPolicy);
                break;
            } else if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                feedJobNotificationHandler.registerFeedIntakeJob(((FeedIntakeOperatorDescriptor) opDesc).getFeedId(),
                        jobId, spec);
                break;
            } else if (opDesc instanceof ChannelMetaOperatorDescriptor) {
                ChannelId channelId = null;
                RepetitiveChannelOperatorDescriptor channelOp = (RepetitiveChannelOperatorDescriptor) ((ChannelMetaOperatorDescriptor) opDesc)
                        .getCoreOperator();
                channelId = (channelOp).getChannelId();
                channelNotificationHandler.registerChannelJob(channelId, jobId, spec);
                break;
            }
        }
    }

    public void setFeedJobState(FeedConnectionId connectionId, JobState jobState) {
        feedJobNotificationHandler.setJobState(connectionId, jobState);
    }

    public JobState getFeedJobState(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getFeedJobState(connectionId);
    }

    public void setChannelJobState(ChannelId channelId, JobState jobState) {
        channelNotificationHandler.setJobState(channelId, jobState);
    }

    public JobState getChannelJobState(ChannelId channelId) {
        return channelNotificationHandler.getChannelJobState(channelId);
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

        Collection<FeedIntakeInfo> intakeInfos = feedJobNotificationHandler.getFeedIntakeInfos();
        Collection<FeedConnectJobInfo> connectJobInfos = feedJobNotificationHandler.getFeedConnectInfos();
        Collection<ChannelJobInfo> channelJobInfos = channelNotificationHandler.getChannelInfos();

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

            for (FeedConnectJobInfo connectInfo : connectJobInfos) {
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
                    feedJobNotificationHandler.deregisterFeedActivity(connectInfo);
                }
            }
            for (ChannelJobInfo channelInfo : channelJobInfos) {
                if (channelInfo.getLocation().contains(deadNode)) {
                    List<ActiveJobInfo> infos = impactedJobs.get(deadNode);
                    if (infos == null) {
                        infos = new ArrayList<ActiveJobInfo>();
                        impactedJobs.put(deadNode, infos);
                    }
                    infos.add(channelInfo);
                    channelInfo.setState(JobState.UNDER_RECOVERY);
                }
            }

        }

        if (impactedJobs.size() > 0) {
            AddNodeWork addNodeWork = new AddNodeWork(deadNodeIds, deadNodeIds.size(), this);
            activeWorkRequestResponseHandler.registerWork(addNodeWork.getWorkId(), impactedJobs);
            workToBeDone.add(addNodeWork);
        }
        return workToBeDone;

    }

    public static class FailureReport {

        private final List<Pair<FeedConnectJobInfo, List<String>>> recoverableConnectJobs;
        private final List<Pair<ChannelJobInfo, List<String>>> recoverableChannelJobs;
        private final Map<IFeedJoint, List<String>> recoverableIntakeFeedIds;

        public FailureReport(Map<IFeedJoint, List<String>> recoverableIntakeFeedIds,
                List<Pair<FeedConnectJobInfo, List<String>>> recoverableSubscribers,
                List<Pair<ChannelJobInfo, List<String>>> recoverableChannelJobs) {
            this.recoverableConnectJobs = recoverableSubscribers;
            this.recoverableIntakeFeedIds = recoverableIntakeFeedIds;
            this.recoverableChannelJobs = recoverableChannelJobs;
        }

        public List<Pair<FeedConnectJobInfo, List<String>>> getRecoverableSubscribers() {
            return recoverableConnectJobs;
        }

        public List<Pair<ChannelJobInfo, List<String>>> getRecoverableChannels() {
            return recoverableChannelJobs;
        }

        public Map<IFeedJoint, List<String>> getRecoverableIntakeFeedIds() {
            return recoverableIntakeFeedIds;
        }

    }

    @Override
    public Set<IClusterManagementWork> notifyNodeJoin(String joinedNodeId) {
        ClusterState newState = AsterixClusterProperties.INSTANCE.getState();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(joinedNodeId + " joined the cluster. " + "Asterix state: " + newState);
        }

        boolean needToReActivateJobs = !newState.equals(state) && (newState == ClusterState.ACTIVE);
        if (needToReActivateJobs) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(joinedNodeId + " Resuming loser jobs (if any)");
            }
            try {
                ActiveJobsActivator activator = new ActiveJobsActivator();
                (new Thread(activator)).start();
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Exception in resuming jobs" + e.getMessage());
                }
            }
            state = newState;
        } else {
            List<ActiveJobInfo> feedsThatCanBeRevived = new ArrayList<ActiveJobInfo>();
            for (Entry<ActiveJobInfo, List<String>> entry : dependentFeeds.entrySet()) {
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
                            LOGGER.info("Exception in resuming jobs" + e.getMessage());
                        }
                    }
                }
                break;
        }

    }

    /*
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
                SessionConfig pc = new SessionConfig(writer, SessionConfig.OutputFormat.ADM);
                try {
                    ctx = MetadataManager.INSTANCE.beginTransaction();
                    FeedId feedId = cInfo.getConnectionId().getFeedId();
                    DisconnectFeedStatement stmt = new DisconnectFeedStatement(new Identifier(feedId.getDataverse()),
                            new Identifier(feedId.getFeedName()), new Identifier(cInfo.getConnectionId().getDatasetName()));
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
        }*/

    public void submitFeedConnectionRequest(IFeedJoint feedPoint, FeedConnectionRequest subscriptionRequest)
            throws Exception {
        feedJobNotificationHandler.submitFeedConnectionRequest(feedPoint, subscriptionRequest);
    }

    public List<FeedConnectionId> getActiveFeedConnections(FeedId feedId) {
        List<FeedConnectionId> connections = new ArrayList<FeedConnectionId>();
        Collection<FeedConnectionId> activeConnections = feedJobNotificationHandler.getActiveFeedConnections();
        if (feedId != null) {
            for (FeedConnectionId connectionId : activeConnections) {
                if (connectionId.getFeedId().equals(feedId)) {
                    connections.add(connectionId);
                }
            }
        } else {
            connections.addAll(activeConnections);
        }
        return connections;
    }

    public List<String> getComputeLocations(FeedId feedId) {
        return feedJobNotificationHandler.getFeedComputeLocations(feedId);
    }

    public List<String> getIntakeLocations(FeedId feedId) {
        return feedJobNotificationHandler.getFeedIntakeLocations(feedId);
    }

    public List<String> getStoreLocations(FeedConnectionId feedConnectionId) {
        return feedJobNotificationHandler.getFeedStorageLocations(feedConnectionId);
    }

    public List<String> getCollectLocations(FeedConnectionId feedConnectionId) {
        return feedJobNotificationHandler.getFeedCollectLocations(feedConnectionId);
    }

    public List<String> getChannelLocations(ChannelId channelId) {
        return channelNotificationHandler.getChannelLocations(channelId);
    }

    public boolean isFeedConnectionActive(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.isFeedConnectionActive(connectionId);
    }

    public void reportPartialDisconnection(FeedConnectionId connectionId) {
        feedJobNotificationHandler.removeFeedJointsPostPipelineTermination(connectionId);
    }

    public void registerFeedReportQueue(FeedConnectionId feedId, LinkedBlockingQueue<String> queue) {
        feedReportQueue.put(feedId, queue);
    }

    public void deregisterFeedReportQueue(FeedConnectionId feedId, LinkedBlockingQueue<String> queue) {
        feedReportQueue.remove(feedId);
    }

    public LinkedBlockingQueue<String> getFeedReportQueue(FeedConnectionId feedId) {
        return feedReportQueue.get(feedId);
    }

    public void registerChannelReportQueue(ChannelId channelId, LinkedBlockingQueue<String> queue) {
        channelReportQueue.put(channelId, queue);
    }

    public void deregisterFeedReportQueue(ChannelId channelId, LinkedBlockingQueue<String> queue) {
        channelReportQueue.remove(channelId);
    }

    public LinkedBlockingQueue<String> getFeedReportQueue(ChannelId channelId) {
        return channelReportQueue.get(channelId);
    }

    public IFeedJoint getAvailableFeedJoint(FeedJointKey feedJointKey) {
        return feedJobNotificationHandler.getAvailableFeedJoint(feedJointKey);
    }

    public boolean isFeedJointAvailable(FeedJointKey feedJointKey) {
        return feedJobNotificationHandler.isFeedPointAvailable(feedJointKey);
    }

    public void registerFeedJoint(IFeedJoint feedJoint) {
        feedJobNotificationHandler.registerFeedJoint(feedJoint);
    }

    public IFeedJoint getFeedJoint(FeedJointKey feedJointKey) {
        return feedJobNotificationHandler.getFeedJoint(feedJointKey);
    }

    public void registerFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber) {
        feedJobNotificationHandler.registerFeedEventSubscriber(connectionId, subscriber);
    }

    public void deregisterFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber) {
        feedJobNotificationHandler.deregisterFeedEventSubscriber(connectionId, subscriber);

    }

    public void registerChannelEventSubscriber(ChannelId channelId, IChannelLifecycleEventSubscriber subscriber) {
        channelNotificationHandler.registerChannelEventSubscriber(channelId, subscriber);
    }

    public void deregisterChannelEventSubscriber(ChannelId channelId, IChannelLifecycleEventSubscriber subscriber) {
        channelNotificationHandler.deregisterChannelEventSubscriber(channelId, subscriber);

    }

    public JobSpecification getCollectJobSpecification(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getCollectJobSpecification(connectionId);
    }

    public JobId getFeedCollectJobId(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getFeedCollectJobId(connectionId);
    }

    public JobSpecification getChannelJobSpecification(ChannelId channelId) {
        return channelNotificationHandler.getChannelJobSpecification(channelId);
    }

}
