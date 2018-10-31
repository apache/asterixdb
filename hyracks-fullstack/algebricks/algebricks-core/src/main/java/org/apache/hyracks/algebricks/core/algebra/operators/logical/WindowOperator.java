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
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * Window operator evaluates window functions. It has the following components:
 * <ul>
 * <li>{@link #partitionExpressions} - define how input data must be partitioned</li>
 * <li>{@link #orderExpressions} - define how data inside these partitions must be ordered</li>
 * <li>{@link #expressions} - window function expressions (running aggregates)</li>
 * <li>{@link #variables} - output variables containing return values of these functions</li>
 * </ul>
 *
 * Window operator does not change cardinality of the input stream.
 */
public class WindowOperator extends AbstractAssignOperator {

    private final List<Mutable<ILogicalExpression>> partitionExpressions;

    private final List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions;

    public WindowOperator(List<Mutable<ILogicalExpression>> partitionExpressions,
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions,
            List<LogicalVariable> variables, List<Mutable<ILogicalExpression>> expressions) {
        super(variables, expressions);
        this.partitionExpressions = new ArrayList<>();
        if (partitionExpressions != null) {
            this.partitionExpressions.addAll(partitionExpressions);
        }
        this.orderExpressions = new ArrayList<>();
        if (orderExpressions != null) {
            this.orderExpressions.addAll(orderExpressions);
        }
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.WINDOW;
    }

    public List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> getOrderExpressions() {
        return orderExpressions;
    }

    public List<Mutable<ILogicalExpression>> getPartitionExpressions() {
        return partitionExpressions;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitWindowOperator(this, arg);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean mod = super.acceptExpressionTransform(visitor);
        for (Mutable<ILogicalExpression> expr : partitionExpressions) {
            mod |= visitor.transform(expr);
        }
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : orderExpressions) {
            mod |= visitor.transform(p.second);
        }
        return mod;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return createVariablePropagationPolicy(true);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = createPropagatingAllInputsTypeEnvironment(ctx);
        int n = variables.size();
        for (int i = 0; i < n; i++) {
            env.setVarType(variables.get(i), ctx.getExpressionTypeComputer().getType(expressions.get(i).getValue(),
                    ctx.getMetadataProvider(), env));
        }
        return env;
    }
}
