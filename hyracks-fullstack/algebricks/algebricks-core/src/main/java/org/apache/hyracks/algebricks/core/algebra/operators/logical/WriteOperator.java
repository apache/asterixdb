/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IWriteDataSink;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class WriteOperator extends AbstractLogicalOperator {
    private final Mutable<ILogicalExpression> sourceExpression;
    private final Mutable<ILogicalExpression> pathExpression;
    private final List<Mutable<ILogicalExpression>> partitionExpressions;
    private final List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions;
    private final IWriteDataSink writeDataSink;

    public WriteOperator(Mutable<ILogicalExpression> sourceExpression, Mutable<ILogicalExpression> pathExpression,
            List<Mutable<ILogicalExpression>> partitionExpressions,
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions,
            IWriteDataSink writeDataSink) {
        this.sourceExpression = sourceExpression;
        this.pathExpression = pathExpression;
        this.partitionExpressions = partitionExpressions;
        this.orderExpressions = orderExpressions;
        this.writeDataSink = writeDataSink;
    }

    public Mutable<ILogicalExpression> getSourceExpression() {
        return sourceExpression;
    }

    public LogicalVariable getSourceVariable() {
        return VariableUtilities.getVariable(sourceExpression.getValue());
    }

    public Mutable<ILogicalExpression> getPathExpression() {
        return pathExpression;
    }

    public List<Mutable<ILogicalExpression>> getPartitionExpressions() {
        return partitionExpressions;
    }

    public List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> getOrderExpressions() {
        return orderExpressions;
    }

    public List<LogicalVariable> getPartitionVariables() {
        List<LogicalVariable> partitionVariables = new ArrayList<>();
        for (Mutable<ILogicalExpression> partitionExpression : partitionExpressions) {
            partitionVariables.add(VariableUtilities.getVariable(partitionExpression.getValue()));
        }
        return partitionVariables;
    }

    public List<OrderColumn> getOrderColumns() {
        List<OrderColumn> orderColumns = new ArrayList<>();
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> orderExpressionPair : orderExpressions) {
            LogicalVariable variable = VariableUtilities.getVariable(orderExpressionPair.getSecond().getValue());
            OrderOperator.IOrder.OrderKind kind = orderExpressionPair.first.getKind();
            orderColumns.add(new OrderColumn(variable, kind));
        }
        return orderColumns;
    }

    public IWriteDataSink getWriteDataSink() {
        return writeDataSink;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.WRITE;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitWriteOperator(this, arg);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean changed = visitor.transform(sourceExpression);
        changed |= visitor.transform(pathExpression);

        for (Mutable<ILogicalExpression> expression : partitionExpressions) {
            changed |= visitor.transform(expression);
        }

        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> orderExpressionPair : orderExpressions) {
            changed |= visitor.transform(orderExpressionPair.second);
        }

        return changed;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.NONE;
    }

    @Override
    public boolean isMap() {
        return true;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<>(inputs.get(0).getValue().getSchema());
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }
}
