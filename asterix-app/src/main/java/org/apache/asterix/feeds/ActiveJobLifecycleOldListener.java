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


/**
 * A listener that subscribes to events associated with cluster membership
 * (nodes joining/leaving the cluster) and job lifecycle (start/end of a job).
 * Subscription to such events allows keeping track of feed ingestion jobs and
 * take any corrective action that may be required when a node involved in a
 * feed leaves the cluster.
 */
/*
public class ActiveJobLifecycleOldListener implements IActiveJobLifeCycleListener {

    private static final Logger LOGGER = Logger.getLogger(ActiveJobLifecycleListener.class.getName());

    public static ActiveJobLifecycleListener INSTANCE = new ActiveJobLifecycleListener();

    private final LinkedBlockingQueue<Message> eventInbox;
    private final LinkedBlockingQueue<IClusterManagementWorkResponse> responseInbox;
    private final Map<FeedCollectInfo, List<String>> dependentFeeds = new HashMap<FeedCollectInfo, List<String>>();
    private final Map<ActiveId, LinkedBlockingQueue<String>> reportQueue;
    private final ActiveJobNotificationHandler jobNotificationHandler;
    private final ActiveWorkRequestResponseHandler activeWorkRequestResponseHandler;
    private final ExecutorService executorService;

    private ClusterState state;

    private ActiveJobLifecycleListener() {
        this.eventInbox = new LinkedBlockingQueue<Message>();
        this.jobNotificationHandler = new ActiveJobNotificationHandler(eventInbox);
        this.responseInbox = new LinkedBlockingQueue<IClusterManagementWorkResponse>();
        this.activeWorkRequestResponseHandler = new ActiveWorkRequestResponseHandler(responseInbox);
        this.reportQueue = new HashMap<ActiveId, LinkedBlockingQueue<String>>();
        this.executorService = Executors.newCachedThreadPool();
        this.executorService.execute(jobNotificationHandler);
        this.executorService.execute(activeWorkRequestResponseHandler);
        ClusterManager.INSTANCE.registerSubscriber(this);
        this.state = AsterixClusterProperties.INSTANCE.getState();
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {
        if (jobNotificationHandler.isRegisteredJob(jobId)) {
            eventInbox.add(new Message(jobId, Message.MessageKind.JOB_START));
        }
    }

    @Override
    public void notifyJobFinish(JobId jobId) throws HyracksException {
        if (jobNotificationHandler.isRegisteredJob(jobId)) {
            eventInbox.add(new Message(jobId, Message.MessageKind.JOB_FINISH));
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("NO NEED TO NOTIFY JOB FINISH!");
            }
        }
    }

    public void registerFeedIntakeProgressTracker(FeedConnectionId connectionId,
            IIntakeProgressTracker feedIntakeProgressTracker) {
        jobNotificationHandler.registerFeedIntakeProgressTracker(connectionId, feedIntakeProgressTracker);
    }

    public void deregisterFeedIntakeProgressTracker(FeedConnectionId connectionId) {
        jobNotificationHandler.deregisterFeedIntakeProgressTracker(connectionId);
    }

    public void updateTrackingInformation(StorageReportFeedMessage srm) {
        jobNotificationHandler.updateTrackingInformation(srm);
    }

    public ActiveJobInfo getActiveJobInfo(ActiveId activeId) {
        return jobNotificationHandler.getActiveJobInfo(activeId);
    }

    /*
     * Traverse job specification to categorize job as a feed intake job or a feed collection job 
     
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
                ActiveId channelId = null;
                RepetitiveChannelOperatorDescriptor channelOp = (RepetitiveChannelOperatorDescriptor) ((ChannelMetaOperatorDescriptor) opDesc)
                        .getCoreOperator();
                channelId = (channelOp).getChannelId();
                jobNotificationHandler.registerActiveJob(channelId, jobId, spec);
                break;
            }
        }
    }

    public void setJobState(ActiveId activeId, JobState jobState) {
        jobNotificationHandler.setJobState(activeId, jobState);
    }

    public JobState getJobState(ActiveId activeId) {
        return jobNotificationHandler.getJobState(activeId);
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
                    // TODO: HANDLE OTHER ACTIVE JOBS!
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
                        jobNotificationHandler.deregisterFeedActivity(connectInfo);
                    }
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
                ActiveId feedId = new ActiveId(cInfo.getConnectionId().getDataverse(), cInfo.getConnectionId()
                        .getName(), cInfo.getConnectionId().getType());
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
                if (connectionId.partOfFeed(feedId)) {
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
    public List<String> getStoreLocations(FeedConnectionId feedConnectionId) {
        return jobNotificationHandler.getFeedStorageLocations(feedConnectionId);
    }

    @Override
    public List<String> getCollectLocations(FeedConnectionId feedConnectionId) {
        return jobNotificationHandler.getFeedCollectLocations(feedConnectionId);
    }

    @Override
    public boolean isActive(ActiveId activeId) {
        return jobNotificationHandler.isActive(activeId);
    }

    public void reportPartialDisconnection(FeedConnectionId connectionId) {
        jobNotificationHandler.removeFeedJointsPostPipelineTermination(connectionId);
    }

    public void registerFeedReportQueue(FeedConnectionId feedId, LinkedBlockingQueue<String> queue) {
        reportQueue.put(feedId, queue);
    }

    public void deregisterFeedReportQueue(FeedConnectionId feedId, LinkedBlockingQueue<String> queue) {
        reportQueue.remove(feedId);
    }

    public LinkedBlockingQueue<String> getFeedReportQueue(FeedConnectionId feedId) {
        return reportQueue.get(feedId);
    }

    @Override
    public IFeedJoint getAvailableFeedJoint(FeedJointKey feedJointKey) {
        return jobNotificationHandler.getAvailableFeedJoint(feedJointKey);
    }

    @Override
    public boolean isFeedJointAvailable(FeedJointKey feedJointKey) {
        return jobNotificationHandler.isFeedPointAvailable(feedJointKey);
    }

    public void registerFeedJoint(IFeedJoint feedJoint) {
        jobNotificationHandler.registerFeedJoint(feedJoint);
    }

    public IFeedJoint getFeedJoint(FeedJointKey feedJointKey) {
        return jobNotificationHandler.getFeedJoint(feedJointKey);
    }

    public void registerFeedEventSubscriber(ActiveId connectionId, IActiveLifecycleEventSubscriber subscriber) {
        jobNotificationHandler.registerEventSubscriber(connectionId, subscriber);
    }

    public void deregisterFeedEventSubscriber(ActiveId connectionId, IActiveLifecycleEventSubscriber subscriber) {
        jobNotificationHandler.deregisterEventSubscriber(connectionId, subscriber);

    }

    public JobSpecification getActiveJobSpecification(ActiveId activeId) {
        return jobNotificationHandler.getActiveJobSpecification(activeId);
    }

    public JobId getFeedCollectJobId(FeedConnectionId connectionId) {
        return jobNotificationHandler.getJobId(connectionId);
    }

    @Override
    public List<String> getLocations(ActiveId activeId) {
        return jobNotificationHandler.getJobLocations(activeId);
    }

}*/
