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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.api.IClusterEventsSubscriber;
import edu.uci.ics.asterix.metadata.api.IClusterManagementWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWorkResponse;
import edu.uci.ics.asterix.metadata.cluster.ClusterManager;
import edu.uci.ics.asterix.metadata.cluster.IClusterManagementWorkResponse;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
import edu.uci.ics.asterix.metadata.feeds.FeedLifecycleListener.FeedFailure.FailureType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.EmptyTupleSourceRuntimeFactory;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
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

public class FeedLifecycleListener implements IJobLifecycleListener, IClusterEventsSubscriber, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(FeedLifecycleListener.class.getName());

    public static FeedLifecycleListener INSTANCE = new FeedLifecycleListener();

    private LinkedBlockingQueue<Message> jobEventInbox;
    private LinkedBlockingQueue<FeedFailureReport> failureEventInbox;
    private Map<Integer, FeedFailureReport> feedsWaitingForResponse = new HashMap<Integer, FeedFailureReport>();

    private FeedLifecycleListener() {
        jobEventInbox = new LinkedBlockingQueue<Message>();
        feedJobNotificationHandler = new FeedJobNotificationHandler(jobEventInbox);
        failureEventInbox = new LinkedBlockingQueue<FeedFailureReport>();
        feedFailureNotificationHandler = new FeedFailureHandler(failureEventInbox);
        new Thread(feedJobNotificationHandler).start();
        new Thread(feedFailureNotificationHandler).start();
        ClusterManager.INSTANCE.registerSubscriber(this);
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
                feedInfo.jobInfo = info;
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
        public JobInfo jobInfo;

        public FeedInfo(FeedId feedId, JobSpecification jobSpec) {
            this.feedId = feedId;
            this.jobSpec = jobSpec;
        }

    }

    @Override
    public Set<IClusterManagementWork> notifyNodeFailure(Set<String> deadNodeIds) {
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

        return handleFailure(failureReport);
    }

    private Set<IClusterManagementWork> handleFailure(FeedFailureReport failureReport) {
        Set<IClusterManagementWork> work = new HashSet<IClusterManagementWork>();
        Map<String, Map<FeedInfo, List<FailureType>>> failureMap = new HashMap<String, Map<FeedInfo, List<FailureType>>>();
        for (Map.Entry<FeedInfo, List<FeedFailure>> entry : failureReport.failures.entrySet()) {
            FeedInfo feedInfo = entry.getKey();
            List<FeedFailure> feedFailures = entry.getValue();
            for (FeedFailure feedFailure : feedFailures) {
                switch (feedFailure.failureType) {
                    case COMPUTE_NODE:
                    case INGESTION_NODE:
                        Map<FeedInfo, List<FailureType>> failuresBecauseOfThisNode = failureMap.get(feedFailure.nodeId);
                        if (failuresBecauseOfThisNode == null) {
                            failuresBecauseOfThisNode = new HashMap<FeedInfo, List<FailureType>>();
                            failuresBecauseOfThisNode.put(feedInfo, new ArrayList<FailureType>());
                            failureMap.put(feedFailure.nodeId, failuresBecauseOfThisNode);
                        }
                        List<FailureType> feedF = failuresBecauseOfThisNode.get(feedInfo);
                        if (feedF == null) {
                            feedF = new ArrayList<FailureType>();
                            failuresBecauseOfThisNode.put(feedInfo, feedF);
                        }
                        feedF.add(feedFailure.failureType);
                        break;
                    case STORAGE_NODE:
                }
            }
        }

        AddNodeWork addNodesWork = new AddNodeWork(failureMap.keySet().size(), this);
        work.add(addNodesWork);
        feedsWaitingForResponse.put(addNodesWork.getWorkId(), failureReport);
        return work;
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

    @Override
    public Set<IClusterManagementWork> notifyNodeJoin(String joinedNodeId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void notifyRequestCompletion(IClusterManagementWorkResponse response) {
        IClusterManagementWork submittedWork = response.getWork();
        switch (submittedWork.getClusterManagementWorkType()) {
            case ADD_NODE:
                AddNodeWorkResponse resp = (AddNodeWorkResponse) response;
                switch (resp.getStatus()) {
                    case FAILURE:
                        break;
                    case SUCCESS:
                        AddNodeWork work = (AddNodeWork) submittedWork;
                        FeedFailureReport failureReport = feedsWaitingForResponse.remove(work.getWorkId());
                        Set<FeedInfo> affectedFeeds = failureReport.failures.keySet();
                        for (FeedInfo feedInfo : affectedFeeds) {
                            try {
                                recoverFeed(feedInfo, resp, failureReport.failures.get(feedInfo));
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    LOGGER.info("Recovered feed:" + feedInfo);
                                }
                            } catch (Exception e) {
                                if (LOGGER.isLoggable(Level.SEVERE)) {
                                    LOGGER.severe("Unable to recover feed:" + feedInfo);
                                }
                            }
                        }
                        break;
                }
                resp.getNodesAdded();
                break;
            case REMOVE_NODE:
                break;
        }
    }

    private void recoverFeed(FeedInfo feedInfo, AddNodeWorkResponse resp, List<FeedFailure> feedFailures)
            throws Exception {
        for (FeedFailure feedFailure : feedFailures) {
            switch (feedFailure.failureType) {
                case INGESTION_NODE:
                    alterFeedJobSpec(feedInfo, resp, feedFailure.nodeId);
                    break;
            }
        }
        JobSpecification spec = feedInfo.jobSpec;
        AsterixAppContextInfo.getInstance().getHcc().startJob(feedInfo.jobSpec);
    }

    private void alterFeedJobSpec(FeedInfo feedInfo, AddNodeWorkResponse resp, String failedNodeId) {
        Random r = new Random();
        Object[] rnodes = resp.getNodesAdded().toArray();
        Node replacementNode = (Node) rnodes[r.nextInt(rnodes.length)];
        String replacementNodeId = AsterixClusterProperties.INSTANCE.getCluster().getInstanceName() + "_"
                + replacementNode.getId();
        Map<OperatorDescriptorId, IOperatorDescriptor> opMap = feedInfo.jobSpec.getOperatorMap();
        Set<Constraint> userConstraints = feedInfo.jobSpec.getUserConstraints();
        List<Constraint> locationConstraintsToReplace = new ArrayList<Constraint>();
        List<Constraint> countConstraintsToReplace = new ArrayList<Constraint>();
        List<OperatorDescriptorId> modifiedOperators = new ArrayList<OperatorDescriptorId>();
        Map<OperatorDescriptorId, List<Constraint>> candidateConstraints = new HashMap<OperatorDescriptorId, List<Constraint>>();
        Map<OperatorDescriptorId, List<String>> newConstraints = new HashMap<OperatorDescriptorId, List<String>>();
        OperatorDescriptorId opId = null;
        for (Constraint constraint : userConstraints) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            switch (lexpr.getTag()) {
                case PARTITION_COUNT:
                    opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                    if (modifiedOperators.contains(opId)) {
                        countConstraintsToReplace.add(constraint);
                    } else {
                        List<Constraint> clist = candidateConstraints.get(opId);
                        if (clist == null) {
                            clist = new ArrayList<Constraint>();
                            candidateConstraints.put(opId, clist);
                        }
                        clist.add(constraint);
                    }
                    break;
                case PARTITION_LOCATION:
                    opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();
                    String oldLocation = (String) ((ConstantExpression) cexpr).getValue();
                    if (oldLocation.equals(failedNodeId)) {
                        locationConstraintsToReplace.add(constraint);
                        modifiedOperators.add(((PartitionLocationExpression) lexpr).getOperatorDescriptorId());
                        List<String> newLocs = newConstraints.get(opId);
                        if (newLocs == null) {
                            newLocs = new ArrayList<String>();
                            newConstraints.put(opId, newLocs);
                        }
                        newLocs.add(replacementNodeId);
                    } else {
                        if (modifiedOperators.contains(opId)) {
                            locationConstraintsToReplace.add(constraint);
                            List<String> newLocs = newConstraints.get(opId);
                            if (newLocs == null) {
                                newLocs = new ArrayList<String>();
                                newConstraints.put(opId, newLocs);
                            }
                            newLocs.add(oldLocation);
                        } else {
                            List<Constraint> clist = candidateConstraints.get(opId);
                            if (clist == null) {
                                clist = new ArrayList<Constraint>();
                                candidateConstraints.put(opId, clist);
                            }
                            clist.add(constraint);
                        }
                    }
                    break;
            }
        }

        feedInfo.jobSpec.getUserConstraints().removeAll(locationConstraintsToReplace);
        feedInfo.jobSpec.getUserConstraints().removeAll(countConstraintsToReplace);

        for (OperatorDescriptorId mopId : modifiedOperators) {
            List<Constraint> clist = candidateConstraints.get(mopId);
            if (clist != null && !clist.isEmpty()) {
                feedInfo.jobSpec.getUserConstraints().removeAll(clist);
            }
        }

        for (Entry<OperatorDescriptorId, List<String>> entry : newConstraints.entrySet()) {
            OperatorDescriptorId nopId = entry.getKey();
            List<String> clist = entry.getValue();
            IOperatorDescriptor op = feedInfo.jobSpec.getOperatorMap().get(nopId);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(feedInfo.jobSpec, op,
                    clist.toArray(new String[] {}));
        }

    }
}
