package edu.uci.ics.asterix.metadata.feeds;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedUtil {

    public static boolean isFeedActive(FeedActivity feedActivity) {
        return (feedActivity != null && !(feedActivity.getActivityType().equals(FeedActivityType.FEED_END) || feedActivity
                .getActivityType().equals(FeedActivityType.FEED_FAILURE)));
    }

    public static void alterJobSpecificationForFeed(JobSpecification spec) {

        Map<OperatorDescriptorId, IOperatorDescriptor> r1 = new HashMap<OperatorDescriptorId, IOperatorDescriptor>();
        Map<OperatorDescriptorId, IOperatorDescriptor> operatorMap = spec.getOperatorMap();
        Map<OperatorDescriptorId, IOperatorDescriptor> opIdToOp = new HashMap<OperatorDescriptorId, IOperatorDescriptor>();
        Map<IOperatorDescriptor, IOperatorDescriptor> opToOp = new HashMap<IOperatorDescriptor, IOperatorDescriptor>();

        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : spec.getOperatorMap().entrySet()) {
            OperatorDescriptorId opId = entry.getKey();
            IOperatorDescriptor op = entry.getValue();
            if (!(op instanceof FeedIntakeOperatorDescriptor)) {
                IOperatorDescriptor newOp = new FeedMetaOperatorDescriptor(spec, op);
                opIdToOp.put(opId, newOp);
                opToOp.put(op, newOp);
            } else {
                opIdToOp.put(opId, op);
                opToOp.put(op, op);
            }
        }

        for (OperatorDescriptorId opId : spec.getOperatorMap().keySet()) {
            operatorMap.put(opId, opIdToOp.get(opId));
        }

        Map<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> connectorOpMap = spec
                .getConnectorOperatorMap();
        Map<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> r2 = new HashMap<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>>();
        for (Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> entry : connectorOpMap
                .entrySet()) {
            IOperatorDescriptor opLeft = entry.getValue().getLeft().getLeft();
            IOperatorDescriptor opRight = entry.getValue().getRight().getLeft();

            if ((!opLeft instanceof FeedIntakeOperatorDescriptor)) {

            }

        }

    }
}
