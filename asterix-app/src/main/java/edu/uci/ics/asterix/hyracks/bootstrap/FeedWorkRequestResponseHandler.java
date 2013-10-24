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
package edu.uci.ics.asterix.hyracks.bootstrap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedFailure;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedFailureReport;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedInfo;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedsDeActivator;
import edu.uci.ics.asterix.metadata.api.IClusterManagementWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWorkResponse;
import edu.uci.ics.asterix.metadata.cluster.IClusterManagementWorkResponse;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression.ExpressionTag;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedWorkRequestResponseHandler implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(FeedWorkRequestResponseHandler.class.getName());

    private final LinkedBlockingQueue<IClusterManagementWorkResponse> inbox;

    private Map<Integer, FeedFailureReport> feedsWaitingForResponse = new HashMap<Integer, FeedFailureReport>();

    public FeedWorkRequestResponseHandler(LinkedBlockingQueue<IClusterManagementWorkResponse> inbox) {
        this.inbox = inbox;
    }

    @Override
    public void run() {
        while (true) {
            IClusterManagementWorkResponse response = null;
            try {
                response = inbox.take();
            } catch (InterruptedException e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Interrupted exception " + e.getMessage());
                }
            }
            IClusterManagementWork submittedWork = response.getWork();
            switch (submittedWork.getClusterManagementWorkType()) {
                case ADD_NODE:
                    AddNodeWorkResponse resp = (AddNodeWorkResponse) response;
                    switch (resp.getStatus()) {
                        case FAILURE:
                            if (LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.warning("Request " + resp.getWork() + " not completed");
                            }
                            break;
                        case SUCCESS:
                            if (LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.warning("Request " + resp.getWork() + " completed");
                            }
                            break;
                    }

                    AddNodeWork work = (AddNodeWork) submittedWork;
                    FeedFailureReport failureReport = feedsWaitingForResponse.remove(work.getWorkId());
                    Set<FeedInfo> affectedFeeds = failureReport.failures.keySet();
                    for (FeedInfo feedInfo : affectedFeeds) {
                        try {
                            recoverFeed(feedInfo, work, resp, failureReport.failures.get(feedInfo));
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
                case REMOVE_NODE:
                    break;
            }
        }
    }

    private void recoverFeed(FeedInfo feedInfo, AddNodeWork work, AddNodeWorkResponse resp,
            List<FeedFailure> feedFailures) throws Exception {
        List<String> failedNodeIds = new ArrayList<String>();
        for (FeedFailure feedFailure : feedFailures) {
            failedNodeIds.add(feedFailure.nodeId);
        }
        List<String> chosenReplacements = new ArrayList<String>();
        switch (resp.getStatus()) {
            case FAILURE:
                for (FeedFailure feedFailure : feedFailures) {
                    switch (feedFailure.failureType) {
                        case INGESTION_NODE:
                            String replacement = getInternalReplacement(feedInfo, feedFailure, failedNodeIds,
                                    chosenReplacements);
                            chosenReplacements.add(replacement);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Existing  node:" + replacement + " chosen to replace "
                                        + feedFailure.nodeId);
                            }
                            alterFeedJobSpec(feedInfo, resp, feedFailure.nodeId, replacement);
                            break;
                    }
                }
                break;
            case SUCCESS:
                List<String> nodesAdded = resp.getNodesAdded();
                int numNodesAdded = nodesAdded.size();
                int nodeIndex = 0;
                for (FeedFailure feedFailure : feedFailures) {
                    switch (feedFailure.failureType) {
                        case INGESTION_NODE:
                            String replacement = null;
                            if (nodeIndex <= numNodesAdded - 1) {
                                replacement = nodesAdded.get(nodeIndex);
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    LOGGER.info("Newly added node:" + replacement + " chosen to replace "
                                            + feedFailure.nodeId);
                                }
                            } else {
                                replacement = getInternalReplacement(feedInfo, feedFailure, failedNodeIds,
                                        chosenReplacements);
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    LOGGER.info("Existing node:" + replacement + " chosen to replace "
                                            + feedFailure.nodeId);
                                }
                                chosenReplacements.add(replacement);
                            }
                            alterFeedJobSpec(feedInfo, resp, feedFailure.nodeId, replacement);
                            nodeIndex++;
                            break;
                        default: // ingestion nodes and compute nodes (in currrent implementation) coincide.
                                 // so correcting ingestion node failure also takes care of compute nodes failure. 
                                 // Storage node failures cannot be recovered from as in current implementation, we 
                                 // do not have data replication.
                    }
                }
                break;
        }

        JobSpecification spec = feedInfo.jobSpec;
        System.out.println("Final recovery Job Spec \n" + spec);
        Thread.sleep(5000);
        AsterixAppContextInfo.getInstance().getHcc().startJob(feedInfo.jobSpec);
    }

    private String getInternalReplacement(FeedInfo feedInfo, FeedFailure feedFailure, List<String> failedNodeIds,
            List<String> chosenReplacements) {
        String failedNodeId = feedFailure.nodeId;
        String replacement = null;;
        // TODO 1st preference is given to any other participant node that is not involved in the feed.
        //      2nd preference is given to a compute node.
        //      3rd preference is given to a storage node
        Set<String> participantNodes = AsterixClusterProperties.INSTANCE.getParticipantNodes();
        if (participantNodes != null && !participantNodes.isEmpty()) {
            List<String> pNodesClone = new ArrayList<String>();
            pNodesClone.addAll(participantNodes);
            pNodesClone.removeAll(feedInfo.storageLocations);
            pNodesClone.removeAll(feedInfo.computeLocations);
            pNodesClone.removeAll(feedInfo.ingestLocations);
            pNodesClone.removeAll(chosenReplacements);

            if (LOGGER.isLoggable(Level.INFO)) {
                for (String candidateNode : pNodesClone) {
                    LOGGER.info("Candidate for replacement:" + candidateNode);
                }
            }
            if (!pNodesClone.isEmpty()) {
                String[] participantNodesArray = pNodesClone.toArray(new String[] {});

                replacement = participantNodesArray[new Random().nextInt(participantNodesArray.length)];
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Participant Node: " + replacement + " chosen as replacement for " + failedNodeId);
                }
            }
        }

        if (replacement == null) {
            feedInfo.computeLocations.removeAll(failedNodeIds);
            boolean computeNodeSubstitute = (feedInfo.computeLocations.size() > 1);
            if (computeNodeSubstitute) {
                replacement = feedInfo.computeLocations.get(0);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Compute node:" + replacement + " chosen to replace " + failedNodeId);
                }
            } else {
                replacement = feedInfo.storageLocations.get(0);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Storage node:" + replacement + " chosen to replace " + failedNodeId);
                }
            }
        }
        return replacement;
    }

    private void alterFeedJobSpec(FeedInfo feedInfo, AddNodeWorkResponse resp, String failedNodeId, String replacement) {
        if (replacement == null) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("Unable to find replacement for failed node :" + failedNodeId);
                LOGGER.severe("Feed: " + feedInfo.feedConnectionId + " will be terminated");
            }
            List<FeedInfo> feedsToTerminate = new ArrayList<FeedInfo>();
            feedsToTerminate.add(feedInfo);
            Thread t = new Thread(new FeedsDeActivator(feedsToTerminate));
            t.start();
        } else {
            replaceNode(feedInfo.jobSpec, failedNodeId, replacement);
        }
    }

    private void replaceNode(JobSpecification jobSpec, String failedNodeId, String replacementNode) {
        Set<Constraint> userConstraints = jobSpec.getUserConstraints();
        List<Constraint> locationConstraintsToReplace = new ArrayList<Constraint>();
        List<Constraint> countConstraintsToReplace = new ArrayList<Constraint>();
        List<OperatorDescriptorId> modifiedOperators = new ArrayList<OperatorDescriptorId>();
        Map<OperatorDescriptorId, List<Constraint>> candidateConstraints = new HashMap<OperatorDescriptorId, List<Constraint>>();
        Map<OperatorDescriptorId, Map<Integer, String>> newConstraints = new HashMap<OperatorDescriptorId, Map<Integer, String>>();
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
                        Map<Integer, String> newLocs = newConstraints.get(opId);
                        if (newLocs == null) {
                            newLocs = new HashMap<Integer, String>();
                            newConstraints.put(opId, newLocs);
                        }
                        int partition = ((PartitionLocationExpression) lexpr).getPartition();
                        newLocs.put(partition, replacementNode);
                    } else {
                        if (modifiedOperators.contains(opId)) {
                            locationConstraintsToReplace.add(constraint);
                            Map<Integer, String> newLocs = newConstraints.get(opId);
                            if (newLocs == null) {
                                newLocs = new HashMap<Integer, String>();
                                newConstraints.put(opId, newLocs);
                            }
                            int partition = ((PartitionLocationExpression) lexpr).getPartition();
                            newLocs.put(partition, oldLocation);
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

        jobSpec.getUserConstraints().removeAll(locationConstraintsToReplace);
        jobSpec.getUserConstraints().removeAll(countConstraintsToReplace);

        for (OperatorDescriptorId mopId : modifiedOperators) {
            List<Constraint> clist = candidateConstraints.get(mopId);
            if (clist != null && !clist.isEmpty()) {
                jobSpec.getUserConstraints().removeAll(clist);

                for (Constraint c : clist) {
                    if (c.getLValue().getTag().equals(ExpressionTag.PARTITION_LOCATION)) {
                        ConstraintExpression cexpr = c.getRValue();
                        int partition = ((PartitionLocationExpression) c.getLValue()).getPartition();
                        String oldLocation = (String) ((ConstantExpression) cexpr).getValue();
                        newConstraints.get(mopId).put(partition, oldLocation);
                    }
                }
            }
        }

        for (Entry<OperatorDescriptorId, Map<Integer, String>> entry : newConstraints.entrySet()) {
            OperatorDescriptorId nopId = entry.getKey();
            Map<Integer, String> clist = entry.getValue();
            IOperatorDescriptor op = jobSpec.getOperatorMap().get(nopId);
            String[] locations = new String[clist.size()];
            for (int i = 0; i < locations.length; i++) {
                locations[i] = clist.get(i);
            }
            PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, op, locations);
        }

    }

    public void registerFeedWork(int workId, FeedFailureReport failureReport) {
        feedsWaitingForResponse.put(workId, failureReport);
    }
}
