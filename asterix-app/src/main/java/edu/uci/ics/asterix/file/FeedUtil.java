package edu.uci.ics.asterix.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
import edu.uci.ics.asterix.metadata.feeds.FeedConnectionId;
import edu.uci.ics.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedMetaOperatorDescriptor;
import edu.uci.ics.asterix.om.types.ARecordType;
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

public class FeedUtil {

    private static Logger LOGGER = Logger.getLogger(FeedUtil.class.getName());

    public static boolean isFeedActive(FeedActivity feedActivity) {
        return (feedActivity != null && !(feedActivity.getActivityType().equals(FeedActivityType.FEED_END) || feedActivity
                .getActivityType().equals(FeedActivityType.FEED_FAILURE)));
    }

    public static JobSpecification alterJobSpecificationForFeed(JobSpecification spec,
            FeedConnectionId feedConnectionId, FeedPolicy feedPolicy) {
        JobSpecification altered = null;
        altered = new JobSpecification();
        Map<OperatorDescriptorId, IOperatorDescriptor> operatorMap = spec.getOperatorMap();

        // copy operators
        Map<OperatorDescriptorId, OperatorDescriptorId> oldNewOID = new HashMap<OperatorDescriptorId, OperatorDescriptorId>();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operatorMap.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                FeedIntakeOperatorDescriptor orig = (FeedIntakeOperatorDescriptor) opDesc;
                FeedIntakeOperatorDescriptor fiop = new FeedIntakeOperatorDescriptor(altered, orig.getFeedId(),
                        orig.getAdapterFactory(), (ARecordType) orig.getAtype(), orig.getRecordDescriptor(),
                        orig.getFeedPolicy());
                oldNewOID.put(opDesc.getOperatorId(), fiop.getOperatorId());
            } else if (opDesc instanceof AsterixLSMTreeInsertDeleteOperatorDescriptor) {
                AsterixLSMTreeInsertDeleteOperatorDescriptor orig = (AsterixLSMTreeInsertDeleteOperatorDescriptor) opDesc;
                AsterixLSMTreeInsertDeleteOperatorDescriptor liop = new AsterixLSMTreeInsertDeleteOperatorDescriptor(
                        altered, orig.getRecordDescriptor(), orig.getStorageManager(),
                        orig.getLifecycleManagerProvider(), orig.getFileSplitProvider(), orig.getTreeIndexTypeTraits(),
                        orig.getComparatorFactories(), orig.getTreeIndexBloomFilterKeyFields(),
                        orig.getFieldPermutations(), orig.getIndexOperation(), orig.getIndexDataflowHelperFactory(),
                        orig.getTupleFilterFactory(), orig.getModificationOpCallbackFactory(), orig.isPrimary());
                oldNewOID.put(opDesc.getOperatorId(), liop.getOperatorId());
            } else {
                FeedMetaOperatorDescriptor metaOp = new FeedMetaOperatorDescriptor(altered, feedConnectionId, opDesc,
                        feedPolicy);
                oldNewOID.put(opDesc.getOperatorId(), metaOp.getOperatorId());
            }
        }

        // copy connectors
        Map<ConnectorDescriptorId, ConnectorDescriptorId> connectorMapping = new HashMap<ConnectorDescriptorId, ConnectorDescriptorId>();
        for (Entry<ConnectorDescriptorId, IConnectorDescriptor> entry : spec.getConnectorMap().entrySet()) {
            IConnectorDescriptor connDesc = entry.getValue();
            ConnectorDescriptorId newConnId = altered.createConnectorDescriptor(connDesc);
            connectorMapping.put(entry.getKey(), newConnId);
        }

        // make connections between operators
        for (Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> entry : spec
                .getConnectorOperatorMap().entrySet()) {
            IConnectorDescriptor connDesc = altered.getConnectorMap().get(connectorMapping.get(entry.getKey()));
            Pair<IOperatorDescriptor, Integer> leftOp = entry.getValue().getLeft();
            Pair<IOperatorDescriptor, Integer> rightOp = entry.getValue().getRight();

            IOperatorDescriptor leftOpDesc = altered.getOperatorMap().get(
                    oldNewOID.get(leftOp.getLeft().getOperatorId()));
            IOperatorDescriptor rightOpDesc = altered.getOperatorMap().get(
                    oldNewOID.get(rightOp.getLeft().getOperatorId()));

            altered.connect(connDesc, leftOpDesc, leftOp.getRight(), rightOpDesc, rightOp.getRight());
        }

        // prepare for setting partition constraints
        Map<OperatorDescriptorId, List<String>> operatorLocations = new HashMap<OperatorDescriptorId, List<String>>();
        Map<OperatorDescriptorId, Integer> operatorCounts = new HashMap<OperatorDescriptorId, Integer>();

        for (Constraint constraint : spec.getUserConstraints()) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            OperatorDescriptorId opId;
            switch (lexpr.getTag()) {
                case PARTITION_COUNT:
                    opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                    if (operatorCounts.get(opId) == null) {
                        operatorCounts.put(opId, 1);
                    } else {
                        operatorCounts.put(opId, operatorCounts.get(opId) + 1);
                    }
                    break;
                case PARTITION_LOCATION:
                    opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();
                    IOperatorDescriptor opDesc = altered.getOperatorMap().get(oldNewOID.get(opId));
                    List<String> locations = operatorLocations.get(opDesc.getOperatorId());
                    if (locations == null) {
                        locations = new ArrayList<String>();
                        operatorLocations.put(opDesc.getOperatorId(), locations);
                    }
                    String location = (String) ((ConstantExpression) cexpr).getValue();
                    locations.add(location);
                    break;
            }
        }

        // set absolute location constraints
        for (Entry<OperatorDescriptorId, List<String>> entry : operatorLocations.entrySet()) {
            IOperatorDescriptor opDesc = altered.getOperatorMap().get(oldNewOID.get(entry.getKey()));
            PartitionConstraintHelper.addAbsoluteLocationConstraint(altered, opDesc,
                    entry.getValue().toArray(new String[] {}));
        }

        // set count constraints
        for (Entry<OperatorDescriptorId, Integer> entry : operatorCounts.entrySet()) {
            IOperatorDescriptor opDesc = altered.getOperatorMap().get(oldNewOID.get(entry.getKey()));
            if (!operatorLocations.keySet().contains(entry.getKey())) {
                PartitionConstraintHelper.addPartitionCountConstraint(altered, opDesc, entry.getValue());
            }
        }

        // useConnectorSchedulingPolicy
        altered.setUseConnectorPolicyForScheduling(spec.isUseConnectorPolicyForScheduling());

        // connectorAssignmentPolicy
        altered.setConnectorPolicyAssignmentPolicy(spec.getConnectorPolicyAssignmentPolicy());

        // roots
        for (OperatorDescriptorId root : spec.getRoots()) {
            altered.addRoot(altered.getOperatorMap().get(oldNewOID.get(root)));
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("New Job Spec:" + altered);
        }

        return altered;

    }

}
