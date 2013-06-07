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
package edu.uci.ics.asterix.metadata.feeds;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.EmptyTupleSourceRuntimeFactory;
import edu.uci.ics.hyracks.api.application.IClusterLifecycleListener;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IActivityClusterGraphGenerator;
import edu.uci.ics.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import edu.uci.ics.hyracks.api.job.IJobLifecycleListener;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobInfo;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;

//import edu.uci.ics.hyracks.api.job.JobInfo;

public class FeedLifecycleListener implements IJobLifecycleListener, IClusterLifecycleListener, Serializable {

    private static final long serialVersionUID = 1L;

    public static FeedLifecycleListener INSTANCE = new FeedLifecycleListener();

    private LinkedBlockingQueue<Message> jobEventInbox;
    private LinkedBlockingQueue<FeedFailureReport> failureEventInbox;

    private FeedLifecycleListener() {
        jobEventInbox = new LinkedBlockingQueue<Message>();
        feedJobNotificationHandler = new FeedJobNotificationHandler(jobEventInbox);
        failureEventInbox = new LinkedBlockingQueue<FeedFailureReport>();
        feedFailureNotificationHandler = new FeedFailureHandler(failureEventInbox);
        new Thread(feedJobNotificationHandler).start();
        new Thread(feedFailureNotificationHandler).start();
    }

    private final FeedJobNotificationHandler feedJobNotificationHandler;
    private final FeedFailureHandler feedFailureNotificationHandler;

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {
        if (feedJobNotificationHandler.isRegisteredFeed(jobId)) {
            jobEventInbox.add(new Message(jobId, Message.MessageKind.JOB_START));
        }
    }

    @Override
    public void notifyJobFinish(JobId jobId) throws HyracksException {
        if (feedJobNotificationHandler.isRegisteredFeed(jobId)) {
            jobEventInbox.add(new Message(jobId, Message.MessageKind.JOB_FINISH));
        }
    }

    @Override
    public void notifyJobCreation(JobId jobId, IActivityClusterGraphGeneratorFactory acggf) throws HyracksException {

        IActivityClusterGraphGenerator acgg = acggf.createActivityClusterGraphGenerator(jobId, AsterixAppContextInfo
                .getInstance().getCCApplicationContext(), EnumSet.noneOf(JobFlag.class));
        JobSpecification spec = acggf.getJobSpecification();
        boolean feedIngestionJob = false;
        FeedId feedId = null;
        for (IOperatorDescriptor opDesc : spec.getOperatorMap().values()) {
            if (!(opDesc instanceof FeedIntakeOperatorDescriptor)) {
                continue;
            }
            feedId = ((FeedIntakeOperatorDescriptor) opDesc).getFeedId();
            feedIngestionJob = true;
            break;
        }
        if (feedIngestionJob) {
            feedJobNotificationHandler.registerFeed(feedId, jobId, spec);
        }

    }

    private static class Message {
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

    public static class FeedFailureReport {
        public Map<FeedInfo, List<FeedFailure>> failures = new HashMap<FeedInfo, List<FeedFailure>>();

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<FeedLifecycleListener.FeedInfo, List<FeedLifecycleListener.FeedFailure>> entry : failures
                    .entrySet()) {
                builder.append(entry.getKey() + " -> failures");
                for (FeedFailure failure : entry.getValue()) {
                    builder.append("failure -> " + failure);
                }
            }
            return builder.toString();
        }
    }

    private static class FeedJobNotificationHandler implements Runnable, Serializable {

        private static final long serialVersionUID = 1L;
        private LinkedBlockingQueue<Message> inbox;
        private Map<JobId, FeedInfo> registeredFeeds = new HashMap<JobId, FeedInfo>();

        public FeedJobNotificationHandler(LinkedBlockingQueue<Message> inbox) {
            this.inbox = inbox;
        }

        public boolean isRegisteredFeed(JobId jobId) {
            return registeredFeeds.containsKey(jobId);
        }

        public void registerFeed(FeedId feedId, JobId jobId, JobSpecification jobSpec) {
            if (registeredFeeds.containsKey(jobId)) {
                throw new IllegalStateException(" Feed already registered ");
            }
            registeredFeeds.put(jobId, new FeedInfo(feedId, jobSpec));
        }

        @Override
        public void run() {
            Message mesg;
            while (true) {
                try {
                    mesg = inbox.take();
                    FeedInfo feedInfo = registeredFeeds.get(mesg.jobId);
                    switch (mesg.messageKind) {
                        case JOB_START:
                            handleJobStartMessage(feedInfo, mesg);
                            break;
                        case JOB_FINISH:
                            handleJobFinishMessage(feedInfo, mesg);
                            break;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }

        private void handleJobStartMessage(FeedInfo feedInfo, Message message) {

            JobSpecification jobSpec = feedInfo.jobSpec;

            List<OperatorDescriptorId> ingestOperatorIds = new ArrayList<OperatorDescriptorId>();
            List<OperatorDescriptorId> computeOperatorIds = new ArrayList<OperatorDescriptorId>();

            Map<OperatorDescriptorId, IOperatorDescriptor> operators = jobSpec.getOperatorMap();
            for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
                if (entry.getValue() instanceof AlgebricksMetaOperatorDescriptor) {
                    AlgebricksMetaOperatorDescriptor op = ((AlgebricksMetaOperatorDescriptor) entry.getValue());
                    IPushRuntimeFactory[] runtimeFactories = op.getPipeline().getRuntimeFactories();
                    for (IPushRuntimeFactory rf : runtimeFactories) {
                        if (rf instanceof EmptyTupleSourceRuntimeFactory) {
                            ingestOperatorIds.add(entry.getKey());
                        } else if (rf instanceof AssignRuntimeFactory) {
                            computeOperatorIds.add(entry.getKey());
                        }
                    }
                }
            }

            try {
                IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
                JobInfo info = hcc.getJobInfo(message.jobId);

                Map<String, String> feedActivityDetails = new HashMap<String, String>();
                StringBuilder ingestLocs = new StringBuilder();
                for (OperatorDescriptorId ingestOpId : ingestOperatorIds) {
                    feedInfo.ingestLocations.addAll(info.getOperatorLocations().get(ingestOpId));
                }
                StringBuilder computeLocs = new StringBuilder();
                for (OperatorDescriptorId computeOpId : computeOperatorIds) {
                    List<String> locations = info.getOperatorLocations().get(computeOpId);
                    if (locations != null) {
                        feedInfo.computeLocations.addAll(locations);
                    } else {
                        feedInfo.computeLocations.addAll(feedInfo.ingestLocations);
                    }
                }

                for (String ingestLoc : feedInfo.ingestLocations) {
                    ingestLocs.append(ingestLoc);
                    ingestLocs.append(",");
                }
                for (String computeLoc : feedInfo.computeLocations) {
                    computeLocs.append(computeLoc);
                    computeLocs.append(",");
                }

                feedActivityDetails.put(FeedActivity.FeedActivityDetails.INGEST_LOCATIONS, ingestLocs.toString());
                feedActivityDetails.put(FeedActivity.FeedActivityDetails.COMPUTE_LOCATIONS, computeLocs.toString());

                FeedActivity feedActivity = new FeedActivity(feedInfo.feedId.getDataverse(),
                        feedInfo.feedId.getDataset(), FeedActivityType.FEED_BEGIN, feedActivityDetails);

                MetadataManager.INSTANCE.acquireWriteLatch();
                MetadataTransactionContext mdTxnCtx = null;
                try {
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    MetadataManager.INSTANCE.registerFeedActivity(mdTxnCtx, new FeedId(feedInfo.feedId.getDataverse(),
                            feedInfo.feedId.getDataset()), feedActivity);
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                } catch (Exception e) {
                    MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                } finally {
                    MetadataManager.INSTANCE.releaseWriteLatch();
                }
            } catch (Exception e) {
                // TODO Add Exception handling here
            }

        }

        private void handleJobFinishMessage(FeedInfo feedInfo, Message message) {

            MetadataManager.INSTANCE.acquireWriteLatch();
            MetadataTransactionContext mdTxnCtx = null;

            try {
                IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
                JobInfo info = hcc.getJobInfo(message.jobId);
                JobStatus status = info.getPendingStatus();
                List<Exception> exceptions;
                boolean failure = status != null && status.equals(JobStatus.FAILURE);
                FeedActivityType activityType = FeedActivityType.FEED_END;
                Map<String, String> details = new HashMap<String, String>();
                if (failure) {
                    exceptions = info.getPendingExceptions();
                    activityType = FeedActivityType.FEED_FAILURE;
                    details.put(FeedActivity.FeedActivityDetails.EXCEPTION_MESSAGE, exceptions.get(0).getMessage());
                }
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                FeedActivity feedActivity = new FeedActivity(feedInfo.feedId.getDataverse(),
                        feedInfo.feedId.getDataset(), activityType, details);
                MetadataManager.INSTANCE.registerFeedActivity(mdTxnCtx, new FeedId(feedInfo.feedId.getDataverse(),
                        feedInfo.feedId.getDataset()), feedActivity);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            } catch (RemoteException | ACIDException | MetadataException e) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                } catch (RemoteException | ACIDException ae) {
                    throw new IllegalStateException(" Unable to abort ");
                }
            } catch (Exception e) {
                // add exception handling here
            } finally {
                MetadataManager.INSTANCE.releaseWriteLatch();
            }

        }
    }

    public static class FeedInfo {
        public FeedId feedId;
        public JobSpecification jobSpec;
        public List<String> ingestLocations = new ArrayList<String>();
        public List<String> computeLocations = new ArrayList<String>();

        public FeedInfo(FeedId feedId, JobSpecification jobSpec) {
            this.feedId = feedId;
            this.jobSpec = jobSpec;
        }

    }

    @Override
    public void notifyNodeJoin(String nodeId, Map<String, String> ncConfiguration) {
        // TODO Auto-generated method stub

    }

    @Override
    public void notifyNodeFailure(Set<String> deadNodeIds) {
        Collection<FeedInfo> feedInfos = feedJobNotificationHandler.registeredFeeds.values();
        FeedFailureReport failureReport = new FeedFailureReport();
        for (FeedInfo feedInfo : feedInfos) {
            for (String deadNodeId : deadNodeIds) {
                if (feedInfo.ingestLocations.contains(deadNodeId)) {
                    List<FeedFailure> failures = failureReport.failures.get(feedInfo);
                    if (failures == null) {
                        failures = new ArrayList<FeedFailure>();
                        failureReport.failures.put(feedInfo, failures);
                    }
                    failures.add(new FeedFailure(FeedFailure.FailureType.INGESTION_NODE, deadNodeId));
                }
                if (feedInfo.computeLocations.contains(deadNodeId)) {
                    List<FeedFailure> failures = failureReport.failures.get(feedInfo);
                    if (failures == null) {
                        failures = new ArrayList<FeedFailure>();
                        failureReport.failures.put(feedInfo, failures);
                    }
                    failures.add(new FeedFailure(FeedFailure.FailureType.COMPUTE_NODE, deadNodeId));
                }
            }
        }
        failureEventInbox.add(failureReport);
    }

    public static class FeedFailure {

        public enum FailureType {
            INGESTION_NODE,
            COMPUTE_NODE,
            STORAGE_NODE
        }

        public FailureType failureType;
        public String nodeId;

        public FeedFailure(FailureType failureType, String nodeId) {
            this.failureType = failureType;
            this.nodeId = nodeId;
        }
    }
}
