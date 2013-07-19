package edu.uci.ics.asterix.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.asterix.algebra.operators.physical.CommitRuntimeFactory;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
import edu.uci.ics.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;

public class FeedUtil {

    private static Logger LOGGER = Logger.getLogger(FeedUtil.class.getName());

    public static boolean isFeedActive(FeedActivity feedActivity) {
        return (feedActivity != null && !(feedActivity.getActivityType().equals(FeedActivityType.FEED_END) || feedActivity
                .getActivityType().equals(FeedActivityType.FEED_FAILURE)));
    }

    public static JobSpecification alterJobSpecificationForFeed2(JobSpecification spec) {
        JobSpecification altered = null;
        altered = new JobSpecification();
        Map<OperatorDescriptorId, IOperatorDescriptor> operatorMap = spec.getOperatorMap();
        Map<OperatorDescriptorId, IOperatorDescriptor> opIdToOp = new HashMap<OperatorDescriptorId, IOperatorDescriptor>();
        Map<IOperatorDescriptor, IOperatorDescriptor> opToOp = new HashMap<IOperatorDescriptor, IOperatorDescriptor>();
        Map<OperatorDescriptorId, OperatorDescriptorId> opIdToOpId = new HashMap<OperatorDescriptorId, OperatorDescriptorId>();

        List<IOperatorDescriptor> opToReplace = new ArrayList<IOperatorDescriptor>();
        Iterator<OperatorDescriptorId> opIt = operatorMap.keySet().iterator();

        while (opIt.hasNext()) {
            OperatorDescriptorId opId = opIt.next();
            IOperatorDescriptor op = operatorMap.get(opId);
            if (op instanceof FeedIntakeOperatorDescriptor) {
                opIdToOp.put(opId, op);
                opToOp.put(op, op);
                opIdToOpId.put(op.getOperatorId(), op.getOperatorId());
                operatorMap.put(opId, op);
            } else if (op instanceof AlgebricksMetaOperatorDescriptor) {
                AlgebricksMetaOperatorDescriptor mop = (AlgebricksMetaOperatorDescriptor) op;
                IPushRuntimeFactory[] runtimeFactories = mop.getPipeline().getRuntimeFactories();
                boolean added = false;
                for (IPushRuntimeFactory rf : runtimeFactories) {
                    if (rf instanceof CommitRuntimeFactory) {
                        opIdToOp.put(opId, op);
                        opToOp.put(op, op);
                        opIdToOpId.put(op.getOperatorId(), op.getOperatorId());
                        operatorMap.put(opId, op);
                        added = true;
                    }
                }
                if (!added) {
                    opToReplace.add(op);
                    IOperatorDescriptor newOp = new FeedMetaOperatorDescriptor(altered, op);
                }
            } else {
                opToReplace.add(op);
                IOperatorDescriptor newOp = new FeedMetaOperatorDescriptor(altered, op);
            }
        }

        // operators that were not changed
        for (OperatorDescriptorId opId : spec.getOperatorMap().keySet()) {
            if (opIdToOp.get(opId) != null) {
                operatorMap.put(opId, opIdToOp.get(opId));
            }
        }

        for (IOperatorDescriptor op : opToReplace) {
            spec.getOperatorMap().remove(op.getOperatorId());
        }

        for (IOperatorDescriptor op : opToReplace) {
            IOperatorDescriptor newOp = new FeedMetaOperatorDescriptor(spec, op);
            opIdToOp.put(op.getOperatorId(), newOp);
            opToOp.put(op, newOp);
            opIdToOpId.put(op.getOperatorId(), newOp.getOperatorId());
        }

    }

    public static void alterJobSpecificationForFeed(JobSpecification spec) {

        Map<OperatorDescriptorId, IOperatorDescriptor> operatorMap = spec.getOperatorMap();
        Map<OperatorDescriptorId, IOperatorDescriptor> opIdToOp = new HashMap<OperatorDescriptorId, IOperatorDescriptor>();
        Map<IOperatorDescriptor, IOperatorDescriptor> opToOp = new HashMap<IOperatorDescriptor, IOperatorDescriptor>();
        Map<OperatorDescriptorId, OperatorDescriptorId> opIdToOpId = new HashMap<OperatorDescriptorId, OperatorDescriptorId>();

        Iterator<OperatorDescriptorId> opIt = operatorMap.keySet().iterator();
        List<IOperatorDescriptor> opToReplace = new ArrayList<IOperatorDescriptor>();
        while (opIt.hasNext()) {
            OperatorDescriptorId opId = opIt.next();
            IOperatorDescriptor op = operatorMap.get(opId);
            if (op instanceof FeedIntakeOperatorDescriptor) {
                opIdToOp.put(opId, op);
                opToOp.put(op, op);
                opIdToOpId.put(op.getOperatorId(), op.getOperatorId());
            } else if (op instanceof AlgebricksMetaOperatorDescriptor) {
                AlgebricksMetaOperatorDescriptor mop = (AlgebricksMetaOperatorDescriptor) op;
                IPushRuntimeFactory[] runtimeFactories = mop.getPipeline().getRuntimeFactories();
                boolean added = false;
                for (IPushRuntimeFactory rf : runtimeFactories) {
                    if (rf instanceof CommitRuntimeFactory) {
                        opIdToOp.put(opId, op);
                        opToOp.put(op, op);
                        opIdToOpId.put(op.getOperatorId(), op.getOperatorId());
                        added = true;
                    }
                }
                if (!added) {
                    opToReplace.add(op);
                }
            } else {
                opToReplace.add(op);
            }
        }

        // operator map
        for (OperatorDescriptorId opId : spec.getOperatorMap().keySet()) {
            if (opIdToOp.get(opId) != null) {
                operatorMap.put(opId, opIdToOp.get(opId));
            }
        }

        for (IOperatorDescriptor op : opToReplace) {
            spec.getOperatorMap().remove(op.getOperatorId());
        }

        for (IOperatorDescriptor op : opToReplace) {
            IOperatorDescriptor newOp = new FeedMetaOperatorDescriptor(spec, op);
            opIdToOp.put(op.getOperatorId(), newOp);
            opToOp.put(op, newOp);
            opIdToOpId.put(op.getOperatorId(), newOp.getOperatorId());
        }

        // connectors
        
        for(Map.Entry<ConnectorDescriptorId, IConnectorDescriptor> entry : spec.getConnectorMap().entrySet()){
            ConnectorDescriptorId cid= entry.getKey();
            IConnectorDescriptor cdesc = entry.getValue();
            if(cdesc instanceof OneToOneConnectorDescriptor){
                ((OneToOneConnectorDescriptor)cdesc).
            }
         }
        
        
        
        
        // connector operator Map
        for (ConnectorDescriptorId cid : spec.getConnectorOperatorMap().keySet()) {
            Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> p = spec
                    .getConnectorOperatorMap().get(cid);

            Pair<IOperatorDescriptor, Integer> leftPair = p.getLeft();
            Pair<IOperatorDescriptor, Integer> newLeftPair = Pair.of(opToOp.get(leftPair.getLeft()),
                    leftPair.getRight());

            Pair<IOperatorDescriptor, Integer> newRightPair = Pair.of(opToOp.get(p.getRight().getLeft()), p.getRight()
                    .getRight());

            spec.getConnectorOperatorMap().put(cid, Pair.of(newLeftPair, newRightPair));
        }

        // operator Output Map
        Set<OperatorDescriptorId> keysForRemoval = new HashSet<OperatorDescriptorId>();
        Map<OperatorDescriptorId, List<IConnectorDescriptor>> keysForAddition = new HashMap<OperatorDescriptorId, List<IConnectorDescriptor>>();
        for (Entry<OperatorDescriptorId, List<IConnectorDescriptor>> entry : spec.getOperatorOutputMap().entrySet()) {
            OperatorDescriptorId opId = entry.getKey();
            if (!opIdToOpId.get(opId).equals(opId)) {
                keysForRemoval.add(opId);
                keysForAddition.put(opIdToOpId.get(opId), entry.getValue());
            }
        }

        for (OperatorDescriptorId opId : keysForRemoval) {
            spec.getOperatorOutputMap().remove(opId);
        }
        
        for(OperatorDescriptorId opId : keysForAddition.keySet()){
            List<IConnectorDescriptor> origConnectors = keysForAddition.get(opId);
            List<IConnectorDescriptor> newConnectors = new ArrayList<IConnectorDescriptor>();
            for(IConnectorDescriptor  connDesc : origConnectors){
                newConnectors.add(e)
            }
                     
        }
        
        
       
        spec.getOperatorOutputMap().putAll(keysForAddition);

        // operator input Map
        keysForRemoval.clear();
        keysForAddition.clear();
        for (Entry<OperatorDescriptorId, List<IConnectorDescriptor>> entry : spec.getOperatorInputMap().entrySet()) {
            OperatorDescriptorId opId = entry.getKey();
            if (!opIdToOpId.get(opId).equals(opId)) {
                keysForRemoval.add(opId);
                keysForAddition.put(opIdToOpId.get(opId), entry.getValue());
            }
        }

        for (OperatorDescriptorId opId : keysForRemoval) {
            spec.getOperatorInputMap().remove(opId);
        }
        spec.getOperatorInputMap().putAll(keysForAddition);

        Set<Constraint> userConstraints = spec.getUserConstraints();
        Set<Constraint> constraintsForRemoval = new HashSet<Constraint>();
        Map<OperatorDescriptorId, List<String>> constraintsForAddition = new HashMap<OperatorDescriptorId, List<String>>();

        OperatorDescriptorId opId;
        for (Constraint constraint : userConstraints) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            switch (lexpr.getTag()) {
                case PARTITION_COUNT:
                    opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                    if (!opIdToOpId.get(opId).equals(opId)) {
                        constraintsForRemoval.add(constraint);
                    }
                    break;
                case PARTITION_LOCATION:
                    opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();
                    if (!opIdToOpId.get(opId).equals(opId)) {
                        constraintsForRemoval.add(constraint);
                        String oldLocation = (String) ((ConstantExpression) cexpr).getValue();
                        List<String> locations = constraintsForAddition.get(opId);
                        if (locations == null) {
                            locations = new ArrayList<String>();
                            constraintsForAddition.put(opId, locations);
                        }
                        locations.add(oldLocation);
                    }
                    break;
            }
        }

        spec.getUserConstraints().removeAll(constraintsForRemoval);
        for (Entry<OperatorDescriptorId, List<String>> entry : constraintsForAddition.entrySet()) {
            OperatorDescriptorId oldOpId = entry.getKey();
            OperatorDescriptorId newOpId = opIdToOpId.get(oldOpId);
            if (!newOpId.equals(oldOpId)) {
                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, opIdToOp.get(oldOpId), entry.getValue()
                        .toArray(new String[] {}));
            }
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Modified job spec with wrapped operators\n" + spec);
        }
    }
}
