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
import edu.uci.ics.asterix.metadata.api.IClusterManagementWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWorkResponse;
import edu.uci.ics.asterix.metadata.cluster.IClusterManagementWorkResponse;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression.ExpressionTag;
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
                case REMOVE_NODE:
                    break;
            }
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
        System.out.println("ALTERED Job Spec" + spec);
        Thread.sleep(3000);
        AsterixAppContextInfo.getInstance().getHcc().startJob(feedInfo.jobSpec);
    }

    private void alterFeedJobSpec(FeedInfo feedInfo, AddNodeWorkResponse resp, String failedNodeId) {
        String replacementNode = null;
        switch (resp.getStatus()) {
            case FAILURE:
                boolean computeNodeSubstitute = (feedInfo.computeLocations.contains(failedNodeId) && feedInfo.computeLocations
                        .size() > 1);
                if (computeNodeSubstitute) {
                    feedInfo.computeLocations.remove(failedNodeId);
                    replacementNode = feedInfo.computeLocations.get(0);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Compute node:" + replacementNode + " chosen to replace " + failedNodeId);
                    }
                } else {
                    replacementNode = feedInfo.storageLocations.get(0);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Storage node:" + replacementNode + " chosen to replace " + failedNodeId);
                    }
                }
                break;
            case SUCCESS:
                Random r = new Random();
                String[] rnodes = resp.getNodesAdded().toArray(new String[] {});
                replacementNode = rnodes[r.nextInt(rnodes.length)];
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Newly added node:" + replacementNode + " chosen to replace " + failedNodeId);
                }

                break;
        }
        replaceNode(feedInfo.jobSpec, failedNodeId, replacementNode);
    }

    private void replaceNode(JobSpecification jobSpec, String failedNodeId, String replacementNode) {
        Map<OperatorDescriptorId, IOperatorDescriptor> opMap = jobSpec.getOperatorMap();
        Set<Constraint> userConstraints = jobSpec.getUserConstraints();
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
                        newLocs.add(replacementNode);
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

        jobSpec.getUserConstraints().removeAll(locationConstraintsToReplace);
        jobSpec.getUserConstraints().removeAll(countConstraintsToReplace);

        for (OperatorDescriptorId mopId : modifiedOperators) {
            List<Constraint> clist = candidateConstraints.get(mopId);
            if (clist != null && !clist.isEmpty()) {
                jobSpec.getUserConstraints().removeAll(clist);

                for (Constraint c : clist) {
                    if (c.getLValue().getTag().equals(ExpressionTag.PARTITION_LOCATION)) {
                        ConstraintExpression cexpr = c.getRValue();
                        String oldLocation = (String) ((ConstantExpression) cexpr).getValue();
                        newConstraints.get(mopId).add(oldLocation);
                    }
                }
            }
        }

        for (Entry<OperatorDescriptorId, List<String>> entry : newConstraints.entrySet()) {
            OperatorDescriptorId nopId = entry.getKey();
            List<String> clist = entry.getValue();
            IOperatorDescriptor op = jobSpec.getOperatorMap().get(nopId);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, op, clist.toArray(new String[] {}));
        }

    }

    public void registerFeedWork(int workId, FeedFailureReport failureReport) {
        feedsWaitingForResponse.put(workId, failureReport);
    }
}
