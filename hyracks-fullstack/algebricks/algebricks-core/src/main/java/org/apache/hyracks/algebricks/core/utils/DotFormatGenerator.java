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
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobActivityGraph;
import org.apache.hyracks.api.job.JobSpecification;

public class DotFormatGenerator {

    private DotFormatGenerator() {
    }

    /**
     * Generates DOT format for {@link JobActivityGraph} that can be visualized
     * using any DOT format visualizer.
     *
     * @param jobActivityGraph The job activity graph
     * @return DOT format
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
     * Generates DOT format for {@link JobSpecification} that can be visualized
     * using any DOT format visualizer.
     *
     * @param jobSpecification The job specification
     * @return DOT format
     */
    public static String generate(final JobSpecification jobSpecification) {
        final DotFormatBuilder graphBuilder = new DotFormatBuilder(DotFormatBuilder.StringValue.of("JobSpecification"));
        final Map<ConnectorDescriptorId, IConnectorDescriptor> connectorMap = jobSpecification.getConnectorMap();
        final Map<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> cOp =
                jobSpecification.getConnectorOperatorMap();
        ConnectorDescriptorId connectorId;
        IConnectorDescriptor connector;
        IOperatorDescriptor leftOperator;
        IOperatorDescriptor rightOperator;
        DotFormatBuilder.Node sourceNode;
        DotFormatBuilder.Node destinationNode;
        String source;
        String destination;
        String edgeLabel;
        for (Map.Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> entry : cOp
                .entrySet()) {
            connectorId = entry.getKey();
            connector = connectorMap.get(connectorId);
            edgeLabel = connector.getClass().getName().substring(connector.getClass().getName().lastIndexOf(".") + 1);
            edgeLabel += "-" + connectorId;
            leftOperator = entry.getValue().getLeft().getLeft();
            rightOperator = entry.getValue().getRight().getLeft();
            source = leftOperator.getClass().getName()
                    .substring(leftOperator.getClass().getName().lastIndexOf(".") + 1);
            sourceNode =
                    graphBuilder.createNode(DotFormatBuilder.StringValue.of(leftOperator.getOperatorId().toString()),
                            DotFormatBuilder.StringValue.of(leftOperator.toString() + "-" + source));
            destination = rightOperator.getClass().getName()
                    .substring(rightOperator.getClass().getName().lastIndexOf(".") + 1);
            destinationNode =
                    graphBuilder.createNode(DotFormatBuilder.StringValue.of(rightOperator.getOperatorId().toString()),
                            DotFormatBuilder.StringValue.of(rightOperator.toString() + "-" + destination));
            graphBuilder.createEdge(sourceNode, destinationNode).setLabel(DotFormatBuilder.StringValue.of(edgeLabel));
        }

        return graphBuilder.getDotDocument();
    }

    /**
     * Generates DOT format for {@link ILogicalPlan} that can be visualized
     * using any DOT format visualizer.
     *
     * @param plan  The logical plan
     * @param dotVisitor    The DOT visitor
     * @return DOT format
     * @throws AlgebricksException
     */
    public static String generate(ILogicalPlan plan, LogicalOperatorDotVisitor dotVisitor) throws AlgebricksException {
        final DotFormatBuilder graphBuilder = new DotFormatBuilder(DotFormatBuilder.StringValue.of("Plan"));
        ILogicalOperator root = plan.getRoots().get(0).getValue();
        generateNode(graphBuilder, root, dotVisitor, new HashSet<>());
        return graphBuilder.getDotDocument();
    }

    public static void generateNode(DotFormatBuilder dotBuilder, ILogicalOperator op,
            LogicalOperatorDotVisitor dotVisitor, Set<ILogicalOperator> operatorsVisited) throws AlgebricksException {
        DotFormatBuilder.StringValue destinationNodeLabel = formatStringOf(op, dotVisitor);
        DotFormatBuilder.Node destinationNode = dotBuilder
                .createNode(DotFormatBuilder.StringValue.of(Integer.toString(op.hashCode())), destinationNodeLabel);
        DotFormatBuilder.StringValue sourceNodeLabel;
        DotFormatBuilder.Node sourceNode;
        for (Mutable<ILogicalOperator> child : op.getInputs()) {
            sourceNodeLabel = formatStringOf(child.getValue(), dotVisitor);
            sourceNode = dotBuilder.createNode(
                    DotFormatBuilder.StringValue.of(Integer.toString(child.getValue().hashCode())), sourceNodeLabel);
            dotBuilder.createEdge(sourceNode, destinationNode);
            if (!operatorsVisited.contains(child.getValue())) {
                generateNode(dotBuilder, child.getValue(), dotVisitor, operatorsVisited);
            }
        }
        if (((AbstractLogicalOperator) op).hasNestedPlans()) {
            ILogicalOperator nestedOperator;
            for (ILogicalPlan nestedPlan : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                nestedOperator = nestedPlan.getRoots().get(0).getValue();
                sourceNodeLabel = formatStringOf(nestedOperator, dotVisitor);
                sourceNode = dotBuilder.createNode(
                        DotFormatBuilder.StringValue.of(Integer.toString(nestedOperator.hashCode())), sourceNodeLabel);
                dotBuilder.createEdge(sourceNode, destinationNode).setLabel(DotFormatBuilder.StringValue.of("subplan"));
                if (!operatorsVisited.contains(nestedOperator)) {
                    generateNode(dotBuilder, nestedOperator, dotVisitor, operatorsVisited);
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
                destinationNodeLabel = formatStringOf(replicateOutput, dotVisitor);
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

    private static DotFormatBuilder.StringValue formatStringOf(ILogicalOperator operator,
            LogicalOperatorDotVisitor dotVisitor) throws AlgebricksException {
        String formattedString = operator.accept(dotVisitor, null).trim();
        IPhysicalOperator physicalOperator = ((AbstractLogicalOperator) operator).getPhysicalOperator();
        if (physicalOperator != null) {
            formattedString += "\\n" + physicalOperator.toString().trim() + " |" + operator.getExecutionMode() + "|";
        } else {
            formattedString += "\\n|" + operator.getExecutionMode() + "|";
        }

        return DotFormatBuilder.StringValue.of(formattedString);
    }
}
