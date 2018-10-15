/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.hyracks.algebricks.core.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.expressions.ConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.LValueConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionCountExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionLocationExpression;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobActivityGraph;
import org.apache.hyracks.api.job.JobSpecification;

public class DotFormatGenerator {

    private final LogicalOperatorDotVisitor dotVisitor = new LogicalOperatorDotVisitor();

    /**
     * Generates DOT format plan for {@link JobActivityGraph} that can be visualized using any DOT format visualizer.
     *
     * @param jobActivityGraph The job activity graph
     * @return DOT format plan
     */
    public static String generate(final JobActivityGraph jobActivityGraph) {
        final DotFormatBuilder graphBuilder = new DotFormatBuilder(DotFormatBuilder.StringValue.of("JobActivityGraph"));
        List<IConnectorDescriptor> connectors;
        IActivity activity;
        ActivityId fromActivityId;
        ActivityId toActivityId;
        String fromFullClassName;
        String toFullClassName;
        String fromClassName;
        String toClassName;
        DotFormatBuilder.Node fromNode;
        DotFormatBuilder.Node toNode;
        final Set<Pair<ActivityId, ActivityId>> activitiesPairedSet = new HashSet<>();
        final Map<ActivityId, IActivity> activityMap = jobActivityGraph.getActivityMap();
        final Map<ActivityId, List<IConnectorDescriptor>> activityInputMap = jobActivityGraph.getActivityInputMap();
        final Map<ActivityId, List<IConnectorDescriptor>> activityOutputMap = jobActivityGraph.getActivityOutputMap();

        // go through each activity. First, map its input -> activity, then activity -> its output
        for (Map.Entry<ActivityId, IActivity> entry : activityMap.entrySet()) {
            toFullClassName = entry.getValue().getClass().getName();
            toClassName = toFullClassName.substring(toFullClassName.lastIndexOf('.') + 1);
            toActivityId = entry.getValue().getActivityId();
            toNode = graphBuilder.createNode(DotFormatBuilder.StringValue.of(toActivityId.toString()),
                    DotFormatBuilder.StringValue.of(toActivityId.toString() + "-" + toClassName));
            // process input -> to activity
            connectors = activityInputMap.get(entry.getKey());
            if (connectors != null) {
                for (IConnectorDescriptor connector : connectors) {
                    fromActivityId = jobActivityGraph.getProducerActivity(connector.getConnectorId());
                    activity = activityMap.get(fromActivityId);
                    fromFullClassName = activity.getClass().getName();
                    fromClassName = fromFullClassName.substring(fromFullClassName.lastIndexOf('.') + 1);
                    fromNode = graphBuilder.createNode(DotFormatBuilder.StringValue.of(fromActivityId.toString()),
                            DotFormatBuilder.StringValue.of(fromActivityId.toString() + "-" + fromClassName));
                    Pair<ActivityId, ActivityId> newPair = new ImmutablePair<>(fromActivityId, toActivityId);
                    if (!activitiesPairedSet.contains(newPair)) {
                        activitiesPairedSet.add(newPair);
                        graphBuilder.createEdge(fromNode, toNode);
                    }
                }
            }

            // process from activity -> output
            fromActivityId = toActivityId;
            fromNode = toNode;
            connectors = activityOutputMap.get(entry.getKey());
            if (connectors != null) {
                for (IConnectorDescriptor connector : connectors) {
                    toActivityId = jobActivityGraph.getConsumerActivity(connector.getConnectorId());
                    activity = activityMap.get(toActivityId);
                    toFullClassName = activity.getClass().getName();
                    toClassName = toFullClassName.substring(toFullClassName.lastIndexOf('.') + 1);
                    toNode = graphBuilder.createNode(DotFormatBuilder.StringValue.of(toActivityId.toString()),
                            DotFormatBuilder.StringValue.of(toActivityId.toString() + "-" + toClassName));
                    Pair<ActivityId, ActivityId> newPair = new ImmutablePair<>(fromActivityId, toActivityId);
                    if (!activitiesPairedSet.contains(newPair)) {
                        activitiesPairedSet.add(newPair);
                        graphBuilder.createEdge(fromNode, toNode);
                    }
                }
            }
        }

        final Map<ActivityId, Set<ActivityId>> blocked2BlockerMap = jobActivityGraph.getBlocked2BlockerMap();
        IActivity blockedActivity;
        for (Map.Entry<ActivityId, Set<ActivityId>> entry : blocked2BlockerMap.entrySet()) {
            blockedActivity = activityMap.get(entry.getKey());
            toFullClassName = blockedActivity.getClass().getName();
            toClassName = toFullClassName.substring(toFullClassName.lastIndexOf('.') + 1);
            toActivityId = entry.getKey();
            toNode = graphBuilder.createNode(DotFormatBuilder.StringValue.of(toActivityId.toString()),
                    DotFormatBuilder.StringValue.of(toActivityId.toString() + "-" + toClassName));
            for (ActivityId blockingActivityId : entry.getValue()) {
                fromActivityId = blockingActivityId;
                activity = activityMap.get(fromActivityId);
                fromFullClassName = activity.getClass().getName();
                fromClassName = fromFullClassName.substring(fromFullClassName.lastIndexOf('.') + 1);
                fromNode = graphBuilder.createNode(DotFormatBuilder.StringValue.of(fromActivityId.toString()),
                        DotFormatBuilder.StringValue.of(fromActivityId.toString() + "-" + fromClassName));
                Pair<ActivityId, ActivityId> newPair = new ImmutablePair<>(fromActivityId, toActivityId);
                if (!activitiesPairedSet.contains(newPair)) {
                    activitiesPairedSet.add(newPair);
                    graphBuilder.createEdge(fromNode, toNode).setColor(DotFormatBuilder.Color.RED);
                }
            }
        }

        return graphBuilder.getDotDocument();
    }

    /**
     * Generates DOT format plan for {@link JobSpecification} that can be visualized using any DOT format visualizer.
     *
     * @param jobSpecification The job specification
     * @return DOT format plan
     */
    public static String generate(final JobSpecification jobSpecification) {
        final DotFormatBuilder graphBuilder = new DotFormatBuilder(DotFormatBuilder.StringValue.of("JobSpecification"));
        final Map<ConnectorDescriptorId, IConnectorDescriptor> connectorMap = jobSpecification.getConnectorMap();
        final Set<Constraint> constraints = jobSpecification.getUserConstraints();
        Map<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> cOp =
                jobSpecification.getConnectorOperatorMap();
        cOp.forEach((connId, srcAndDest) -> addToGraph(graphBuilder, constraints, connectorMap, connId, srcAndDest));
        return graphBuilder.getDotDocument();
    }

    /**
     * Generates DOT format plan for {@link ILogicalPlan} that can be visualized using any DOT format visualizer.
     *
     * @param plan  The logical plan
     * @param showDetails whether to show the details of the operator like physical properties
     * @return DOT format plan
     * @throws AlgebricksException When one operator throws an exception while visiting it.
     */
    public String generate(ILogicalPlan plan, boolean showDetails) throws AlgebricksException {
        ILogicalOperator root = plan.getRoots().get(0).getValue();
        return generate(root, showDetails);
    }

    /**
     * Generates DOT format plan considering "startingOp" as the root operator.
     *
     * @param startingOp the starting operator
     * @param showDetails whether to show the details of the operator like physical properties
     * @return DOT format plan
     * @throws AlgebricksException When one operator throws an exception while visiting it.
     */
    public String generate(ILogicalOperator startingOp, boolean showDetails) throws AlgebricksException {
        final DotFormatBuilder graphBuilder = new DotFormatBuilder(DotFormatBuilder.StringValue.of("Plan"));
        generateNode(graphBuilder, startingOp, showDetails, new HashSet<>());
        return graphBuilder.getDotDocument();
    }

    private void generateNode(DotFormatBuilder dotBuilder, ILogicalOperator op, boolean showDetails,
            Set<ILogicalOperator> operatorsVisited) throws AlgebricksException {
        DotFormatBuilder.StringValue destinationNodeLabel = formatStringOf(op, showDetails);
        DotFormatBuilder.Node destinationNode = dotBuilder
                .createNode(DotFormatBuilder.StringValue.of(Integer.toString(op.hashCode())), destinationNodeLabel);
        DotFormatBuilder.StringValue sourceNodeLabel;
        DotFormatBuilder.Node sourceNode;
        for (Mutable<ILogicalOperator> child : op.getInputs()) {
            sourceNodeLabel = formatStringOf(child.getValue(), showDetails);
            sourceNode = dotBuilder.createNode(
                    DotFormatBuilder.StringValue.of(Integer.toString(child.getValue().hashCode())), sourceNodeLabel);
            dotBuilder.createEdge(sourceNode, destinationNode);
            if (!operatorsVisited.contains(child.getValue())) {
                generateNode(dotBuilder, child.getValue(), showDetails, operatorsVisited);
            }
        }
        if (((AbstractLogicalOperator) op).hasNestedPlans()) {
            ILogicalOperator nestedOperator;
            for (ILogicalPlan nestedPlan : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                nestedOperator = nestedPlan.getRoots().get(0).getValue();
                sourceNodeLabel = formatStringOf(nestedOperator, showDetails);
                sourceNode = dotBuilder.createNode(
                        DotFormatBuilder.StringValue.of(Integer.toString(nestedOperator.hashCode())), sourceNodeLabel);
                dotBuilder.createEdge(sourceNode, destinationNode).setLabel(DotFormatBuilder.StringValue.of("subplan"));
                if (!operatorsVisited.contains(nestedOperator)) {
                    generateNode(dotBuilder, nestedOperator, showDetails, operatorsVisited);
                }
            }
        }
        if (!(op instanceof ExchangeOperator)) {
            destinationNode.setFillColor(DotFormatBuilder.Color.SKYBLUE);
        }

        // replicate/split operator
        if (op.getOperatorTag() == LogicalOperatorTag.REPLICATE || op.getOperatorTag() == LogicalOperatorTag.SPLIT) {
            AbstractReplicateOperator replicateOperator = (AbstractReplicateOperator) op;
            ILogicalOperator replicateOutput;
            sourceNode = destinationNode;
            for (int i = 0; i < replicateOperator.getOutputs().size(); i++) {
                replicateOutput = replicateOperator.getOutputs().get(i).getValue();
                destinationNodeLabel = formatStringOf(replicateOutput, showDetails);
                destinationNode = dotBuilder.createNode(
                        DotFormatBuilder.StringValue.of(Integer.toString(replicateOutput.hashCode())),
                        destinationNodeLabel);
                if (replicateOperator.getOutputMaterializationFlags()[i]) {
                    dotBuilder.createEdge(sourceNode, destinationNode).setColor(DotFormatBuilder.Color.RED);
                } else {
                    dotBuilder.createEdge(sourceNode, destinationNode);
                }
            }
        }

        operatorsVisited.add(op);
    }

    private DotFormatBuilder.StringValue formatStringOf(ILogicalOperator operator, boolean showDetails)
            throws AlgebricksException {
        String formattedString = operator.accept(dotVisitor, showDetails).trim();
        return DotFormatBuilder.StringValue.of(formattedString);
    }

    private static void addToGraph(DotFormatBuilder graph, Set<Constraint> constraints,
            Map<ConnectorDescriptorId, IConnectorDescriptor> connMap, ConnectorDescriptorId connId,
            Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> srcAndDest) {
        IConnectorDescriptor connector = connMap.get(connId);
        String edgeLabel;
        edgeLabel = connector.getClass().getName().substring(connector.getClass().getName().lastIndexOf(".") + 1);
        edgeLabel += "-" + connId;
        IOperatorDescriptor sourceOp = srcAndDest.getLeft().getLeft();
        IOperatorDescriptor destOp = srcAndDest.getRight().getLeft();
        StringBuilder source = new StringBuilder(
                sourceOp.getClass().getName().substring(sourceOp.getClass().getName().lastIndexOf(".") + 1));
        StringBuilder destination = new StringBuilder(
                destOp.getClass().getName().substring(destOp.getClass().getName().lastIndexOf(".") + 1));
        // constraints
        for (Constraint constraint : constraints) {
            LValueConstraintExpression lvalue = constraint.getLValue();
            if (lvalue.getTag() == ConstraintExpression.ExpressionTag.PARTITION_COUNT) {
                PartitionCountExpression count = (PartitionCountExpression) lvalue;
                if (count.getOperatorDescriptorId().equals(sourceOp.getOperatorId())) {
                    source.append("\n").append(constraint);
                }
                if (count.getOperatorDescriptorId().equals(destOp.getOperatorId())) {
                    destination.append("\n").append(constraint);
                }
            } else if (lvalue.getTag() == ConstraintExpression.ExpressionTag.PARTITION_LOCATION) {
                PartitionLocationExpression location = (PartitionLocationExpression) lvalue;
                if (location.getOperatorDescriptorId().equals(sourceOp.getOperatorId())) {
                    source.append("\n").append(constraint);
                }
                if (location.getOperatorDescriptorId().equals(destOp.getOperatorId())) {
                    destination.append("\n").append(constraint);
                }
            }
        }
        DotFormatBuilder.Node sourceNode =
                graph.createNode(DotFormatBuilder.StringValue.of(sourceOp.getOperatorId().toString()),
                        DotFormatBuilder.StringValue.of(sourceOp.toString() + "-" + source));
        DotFormatBuilder.Node destinationNode =
                graph.createNode(DotFormatBuilder.StringValue.of(destOp.getOperatorId().toString()),
                        DotFormatBuilder.StringValue.of(destOp.toString() + "-" + destination));
        graph.createEdge(sourceNode, destinationNode).setLabel(DotFormatBuilder.StringValue.of(edgeLabel));
    }
}
