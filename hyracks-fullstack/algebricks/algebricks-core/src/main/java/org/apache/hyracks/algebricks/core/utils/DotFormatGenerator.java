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
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.api.util.DotFormatBuilder;

public class DotFormatGenerator {

    private final LogicalOperatorDotVisitor dotVisitor = new LogicalOperatorDotVisitor();

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
}
