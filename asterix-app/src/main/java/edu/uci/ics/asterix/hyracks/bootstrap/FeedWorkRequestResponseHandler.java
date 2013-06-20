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
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
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
        //AsterixAppContextInfo.getInstance().getHcc().startJob(feedInfo.jobSpec);
    }

    private void alterFeedJobSpec(FeedInfo feedInfo, AddNodeWorkResponse resp, String failedNodeId) {
        Random r = new Random();
        String[] rnodes = resp.getNodesAdded().toArray(new String[] {});
        String replacementNode = rnodes[r.nextInt(rnodes.length)];
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

    public void registerFeedWork(int workId, FeedFailureReport failureReport) {
        feedsWaitingForResponse.put(workId, failureReport);
    }
}
