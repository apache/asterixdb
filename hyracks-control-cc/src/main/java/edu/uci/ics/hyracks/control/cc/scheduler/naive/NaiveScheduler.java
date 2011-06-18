package edu.uci.ics.hyracks.control.cc.scheduler.naive;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.constraints.expressions.BelongsToExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression.ExpressionTag;
import edu.uci.ics.hyracks.api.constraints.expressions.EnumeratedCollectionExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.RelationalExpression;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.JobAttempt;
import edu.uci.ics.hyracks.control.cc.job.JobStageAttempt;
import edu.uci.ics.hyracks.control.cc.scheduler.IJobAttemptSchedulerState;
import edu.uci.ics.hyracks.control.cc.scheduler.ISchedule;
import edu.uci.ics.hyracks.control.cc.scheduler.IScheduler;

public class NaiveScheduler implements IScheduler {
    private static final Logger LOGGER = Logger.getLogger(NaiveScheduler.class.getName());

    private final ClusterControllerService ccs;

    public NaiveScheduler(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public IJobAttemptSchedulerState createJobAttemptState(JobAttempt ja) {
        return new JobAttemptState(ja.getJobRun().getConstraints());
    }

    @Override
    public void schedule(Set<JobStageAttempt> runnableStageAttempts) throws HyracksException {
        for (JobStageAttempt jsa : runnableStageAttempts) {
            Set<OperatorDescriptorId> operators = new HashSet<OperatorDescriptorId>();
            for (ActivityNodeId aid : jsa.getJobStage().getTasks()) {
                operators.add(aid.getOperatorDescriptorId());
            }
            jsa.setSchedule(computeSchedule(jsa, operators));
        }
    }

    private ISchedule computeSchedule(JobStageAttempt jsa, Set<OperatorDescriptorId> operators) throws HyracksException {
        Set<String> nodeSet = ccs.getNodeMap().keySet();
        if (nodeSet.isEmpty()) {
            throw new HyracksException("0 usable nodes found");
        }
        String[] liveNodes = ccs.getNodeMap().keySet().toArray(new String[nodeSet.size()]);
        JobAttempt ja = jsa.getJobAttempt();
        final JobAttemptState jas = (JobAttemptState) ja.getSchedulerState();

        List<PartitionLocationExpression> rrAssignment = new ArrayList<PartitionLocationExpression>();

        for (OperatorDescriptorId oid : operators) {
            String[] opParts = null;
            if (!jas.allocations.containsKey(oid)) {
                Set<ConstraintExpression> opConstraints = jas.opConstraints.get(oid);
                for (ConstraintExpression ce : opConstraints) {
                    int nParts = getNumPartitions(oid, ce);
                    if (nParts != -1) {
                        opParts = new String[nParts];
                        break;
                    }
                }
                if (opParts == null) {
                    throw new HyracksException("Unable to satisfy constraints for operator : " + oid);
                }
                jas.allocations.put(oid, opParts);
                BitSet unassignedPartsIds = new BitSet(opParts.length);
                unassignedPartsIds.set(0, opParts.length);
                for (ConstraintExpression ce : opConstraints) {
                    if (ce.getTag() == ConstraintExpression.ExpressionTag.BELONGS_TO) {
                        BelongsToExpression bE = (BelongsToExpression) ce;
                        if (bE.getItemExpression().getTag() == ConstraintExpression.ExpressionTag.PARTITION_LOCATION) {
                            PartitionLocationExpression plE = (PartitionLocationExpression) bE.getItemExpression();
                            if (plE.getOperatorDescriptorId().equals(oid)) {
                                int part = plE.getPartition();
                                if (bE.getSetExpression().getTag() == ConstraintExpression.ExpressionTag.ENUMERATED_SET) {
                                    EnumeratedCollectionExpression ecE = (EnumeratedCollectionExpression) bE
                                            .getSetExpression();
                                    for (ConstraintExpression value : ecE.getMembers()) {
                                        if (value.getTag() == ConstraintExpression.ExpressionTag.CONSTANT) {
                                            ConstantExpression nodeConst = (ConstantExpression) value;
                                            String nodeId = (String) nodeConst.getValue();
                                            if (nodeSet.contains(nodeId)) {
                                                opParts[part] = nodeId;
                                                unassignedPartsIds.clear(part);
                                                LOGGER.info("Assigned: " + oid + ":" + part + ": " + nodeId);
                                                break;
                                            }
                                        }
                                    }
                                }
                                if (unassignedPartsIds.get(part)) {
                                    throw new HyracksException("Unsatisfiable constraint for operator: " + oid);
                                }
                            }
                        }
                    }
                }

                if (!unassignedPartsIds.isEmpty()) {
                    // Do round robin assignment.
                    for (int i = unassignedPartsIds.nextSetBit(0); i >= 0; i = unassignedPartsIds.nextSetBit(i + 1)) {
                        rrAssignment.add(new PartitionLocationExpression(oid, i));
                    }
                }
            }
        }
        int n = rrAssignment.size();
        for (int i = 0; i < n; ++i) {
            PartitionLocationExpression plE = rrAssignment.get(i);
            String[] opParts = jas.allocations.get(plE.getOperatorDescriptorId());
            String node = liveNodes[i % liveNodes.length];
            LOGGER.info("Assigned: " + plE.getOperatorDescriptorId() + ":" + plE.getPartition() + ": " + node);
            opParts[plE.getPartition()] = node;
        }
        return new ISchedule() {
            @Override
            public String[] getPartitions(ActivityNodeId aid) {
                return jas.allocations.get(aid.getOperatorDescriptorId());
            }
        };
    }

    private int getNumPartitions(OperatorDescriptorId oid, ConstraintExpression ce) {
        if (ce.getTag() == ExpressionTag.RELATIONAL) {
            RelationalExpression re = (RelationalExpression) ce;
            if (re.getOperator() == RelationalExpression.Operator.EQUAL) {
                if (re.getLeft().getTag() == ConstraintExpression.ExpressionTag.PARTITION_COUNT) {
                    return getNumPartitions(oid, (PartitionCountExpression) re.getLeft(), re.getRight());
                } else if (re.getRight().getTag() == ConstraintExpression.ExpressionTag.PARTITION_COUNT) {
                    return getNumPartitions(oid, (PartitionCountExpression) re.getRight(), re.getLeft());
                }
            }
        }
        return -1;
    }

    private int getNumPartitions(OperatorDescriptorId oid, PartitionCountExpression pce, ConstraintExpression ce) {
        if (pce.getOperatorDescriptorId().equals(oid)) {
            if (ce.getTag() == ConstraintExpression.ExpressionTag.CONSTANT) {
                ConstantExpression constExpr = (ConstantExpression) ce;
                Integer n = (Integer) constExpr.getValue();
                return n.intValue();
            }
        }
        return -1;
    }

    private static class JobAttemptState implements IJobAttemptSchedulerState {
        final Map<OperatorDescriptorId, String[]> allocations;
        final Map<OperatorDescriptorId, Set<ConstraintExpression>> opConstraints;

        public JobAttemptState(Set<ConstraintExpression> constraints) {
            allocations = new HashMap<OperatorDescriptorId, String[]>();
            opConstraints = new HashMap<OperatorDescriptorId, Set<ConstraintExpression>>();
            List<ConstraintExpression> ceList = new ArrayList<ConstraintExpression>();
            for (ConstraintExpression ce : constraints) {
                ceList.clear();
                ceList.add(ce);
                getAllConstraints(ceList);
                for (ConstraintExpression ce2 : ceList) {
                    switch (ce2.getTag()) {
                        case PARTITION_COUNT:
                            addToOpConstraints(opConstraints,
                                    ((PartitionCountExpression) ce2).getOperatorDescriptorId(), ce);
                            break;

                        case PARTITION_LOCATION:
                            addToOpConstraints(opConstraints,
                                    ((PartitionLocationExpression) ce2).getOperatorDescriptorId(), ce);
                            break;
                    }
                }
            }
        }

        private static void addToOpConstraints(Map<OperatorDescriptorId, Set<ConstraintExpression>> opc,
                OperatorDescriptorId opId, ConstraintExpression ce) {
            Set<ConstraintExpression> opSet = opc.get(opId);
            if (opSet == null) {
                opSet = new HashSet<ConstraintExpression>();
                opc.put(opId, opSet);
            }
            opSet.add(ce);
        }

        private static void getAllConstraints(List<ConstraintExpression> ceList) {
            for (int i = 0; i < ceList.size(); ++i) {
                ConstraintExpression cE = ceList.get(i);
                cE.getChildren(ceList);
            }
        }
    }
}
