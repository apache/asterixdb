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
import org.apache.asterix.aql.translator.AqlTranslator;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.IClusterManagementWorkResponse;
import org.apache.asterix.common.feeds.FeedConnectJobInfo;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedConnectionRequest;
import org.apache.asterix.common.feeds.FeedId;
import org.apache.asterix.common.feeds.FeedIntakeInfo;
import org.apache.asterix.common.feeds.FeedJobInfo;
import org.apache.asterix.common.feeds.FeedJobInfo.FeedJobState;
import org.apache.asterix.common.feeds.FeedJointKey;
import org.apache.asterix.common.feeds.api.IFeedJoint;
import org.apache.asterix.common.feeds.api.IFeedLifecycleEventSubscriber;
import org.apache.asterix.common.feeds.api.IFeedLifecycleListener;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.common.feeds.message.StorageReportFeedMessage;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.DisconnectFeedStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
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
public class FeedLifecycleListener implements IFeedLifecycleListener {

    private static final Logger LOGGER = Logger.getLogger(FeedLifecycleListener.class.getName());

    public static FeedLifecycleListener INSTANCE = new FeedLifecycleListener();

    private final LinkedBlockingQueue<Message> jobEventInbox;
    private final LinkedBlockingQueue<IClusterManagementWorkResponse> responseInbox;
    private final Map<FeedCollectInfo, List<String>> dependentFeeds = new HashMap<FeedCollectInfo, List<String>>();
    private final Map<FeedConnectionId, LinkedBlockingQueue<String>> feedReportQueue;
    private final FeedJobNotificationHandler feedJobNotificationHandler;
    private final FeedWorkRequestResponseHandler feedWorkRequestResponseHandler;
    private final ExecutorService executorService;

    private ClusterState state;

    private FeedLifecycleListener() {
        this.jobEventInbox = new LinkedBlockingQueue<Message>();
        this.feedJobNotificationHandler = new FeedJobNotificationHandler(jobEventInbox);
        this.responseInbox = new LinkedBlockingQueue<IClusterManagementWorkResponse>();
        this.feedWorkRequestResponseHandler = new FeedWorkRequestResponseHandler(responseInbox);
        this.feedReportQueue = new HashMap<FeedConnectionId, LinkedBlockingQueue<String>>();
        this.executorService = Executors.newCachedThreadPool();
        this.executorService.execute(feedJobNotificationHandler);
        this.executorService.execute(feedWorkRequestResponseHandler);
        ClusterManager.INSTANCE.registerSubscriber(this);
        this.state = AsterixClusterProperties.INSTANCE.getState();
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {
        if (feedJobNotificationHandler.isRegisteredFeedJob(jobId)) {
            jobEventInbox.add(new Message(jobId, Message.MessageKind.JOB_START));
        }
    }

    @Override
    public void notifyJobFinish(JobId jobId) throws HyracksException {
        if (feedJobNotificationHandler.isRegisteredFeedJob(jobId)) {
            jobEventInbox.add(new Message(jobId, Message.MessageKind.JOB_FINISH));
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
            }
        }
    }

    public void setJobState(FeedConnectionId connectionId, FeedJobState jobState) {
        feedJobNotificationHandler.setJobState(connectionId, jobState);
    }

    public FeedJobState getFeedJobState(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getFeedJobState(connectionId);
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

        Map<String, List<FeedJobInfo>> impactedJobs = new HashMap<String, List<FeedJobInfo>>();

        for (String deadNode : deadNodeIds) {
            for (FeedIntakeInfo intakeInfo : intakeInfos) {
                if (intakeInfo.getIntakeLocation().contains(deadNode)) {
                    List<FeedJobInfo> infos = impactedJobs.get(deadNode);
                    if (infos == null) {
                        infos = new ArrayList<FeedJobInfo>();
                        impactedJobs.put(deadNode, infos);
                    }
                    infos.add(intakeInfo);
                    intakeInfo.setState(FeedJobState.UNDER_RECOVERY);
                }
            }

            for (FeedConnectJobInfo connectInfo : connectJobInfos) {
                if (connectInfo.getStorageLocations().contains(deadNode)) {
                    continue;
                }
                if (connectInfo.getComputeLocations().contains(deadNode)
                        || connectInfo.getCollectLocations().contains(deadNode)) {
                    List<FeedJobInfo> infos = impactedJobs.get(deadNode);
                    if (infos == null) {
                        infos = new ArrayList<FeedJobInfo>();
                        impactedJobs.put(deadNode, infos);
                    }
                    infos.add(connectInfo);
                    connectInfo.setState(FeedJobState.UNDER_RECOVERY);
                    feedJobNotificationHandler.deregisterFeedActivity(connectInfo);
                }
            }

        }

        if (impactedJobs.size() > 0) {
            AddNodeWork addNodeWork = new AddNodeWork(deadNodeIds, deadNodeIds.size(), this);
            feedWorkRequestResponseHandler.registerFeedWork(addNodeWork.getWorkId(), impactedJobs);
            workToBeDone.add(addNodeWork);
        }
        return workToBeDone;

    }

    public static class FailureReport {

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
                FeedsActivator activator = new FeedsActivator();
                (new Thread(activator)).start();
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Exception in resuming feeds" + e.getMessage());
                }
            }
            state = newState;
        } else {
            List<FeedCollectInfo> feedsThatCanBeRevived = new ArrayList<FeedCollectInfo>();
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
                FeedsActivator activator = new FeedsActivator(feedsThatCanBeRevived);
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
                        FeedsActivator activator = new FeedsActivator();
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
    }

    public void submitFeedConnectionRequest(IFeedJoint feedPoint, FeedConnectionRequest subscriptionRequest)
            throws Exception {
        feedJobNotificationHandler.submitFeedConnectionRequest(feedPoint, subscriptionRequest);
    }

    @Override
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

    @Override
    public List<String> getComputeLocations(FeedId feedId) {
        return feedJobNotificationHandler.getFeedComputeLocations(feedId);
    }

    @Override
    public List<String> getIntakeLocations(FeedId feedId) {
        return feedJobNotificationHandler.getFeedIntakeLocations(feedId);
    }

    @Override
    public List<String> getStoreLocations(FeedConnectionId feedConnectionId) {
        return feedJobNotificationHandler.getFeedStorageLocations(feedConnectionId);
    }

    @Override
    public List<String> getCollectLocations(FeedConnectionId feedConnectionId) {
        return feedJobNotificationHandler.getFeedCollectLocations(feedConnectionId);
    }

    @Override
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

    @Override
    public IFeedJoint getAvailableFeedJoint(FeedJointKey feedJointKey) {
        return feedJobNotificationHandler.getAvailableFeedJoint(feedJointKey);
    }

    @Override
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

    public JobSpecification getCollectJobSpecification(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getCollectJobSpecification(connectionId);
    }

    public JobId getFeedCollectJobId(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getFeedCollectJobId(connectionId);
    }

}
