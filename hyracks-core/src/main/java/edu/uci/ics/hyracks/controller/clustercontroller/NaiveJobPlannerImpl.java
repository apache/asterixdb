package edu.uci.ics.hyracks.controller.clustercontroller;

import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobStage;
import edu.uci.ics.hyracks.dataflow.base.IOperatorDescriptorVisitor;
import edu.uci.ics.hyracks.dataflow.util.PlanUtils;

public class NaiveJobPlannerImpl implements IJobPlanner {
    @Override
    public Set<String> plan(JobControl jc, JobStage stage) throws Exception {
        final Set<OperatorDescriptorId> opSet = new HashSet<OperatorDescriptorId>();
        for (ActivityNodeId t : stage.getTasks()) {
            opSet.add(jc.getJobPlan().getActivityNodeMap().get(t).getOwner().getOperatorId());
        }

        final Set<String> candidateNodes = new HashSet<String>();

        IOperatorDescriptorVisitor visitor = new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) throws Exception {
                if (!opSet.contains(op.getOperatorId())) {
                    return;
                }
                String[] partitions = op.getPartitions();
                if (partitions == null) {
                    PartitionConstraint pc = op.getPartitionConstraint();
                    LocationConstraint[] lcs = pc.getLocationConstraints();
                    String[] assignment = new String[lcs.length];
                    for (int i = 0; i < lcs.length; ++i) {
                        String nodeId = ((AbsoluteLocationConstraint) lcs[i]).getLocationId();
                        assignment[i] = nodeId;
                    }
                    op.setPartitions(assignment);
                    partitions = assignment;
                }
                for (String p : partitions) {
                    candidateNodes.add(p);
                }
            }
        };

        PlanUtils.visit(jc.getJobPlan().getJobSpecification(), visitor);
        return candidateNodes;
    }
}