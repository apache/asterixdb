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
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * Window operator evaluates window functions. It has the following components:
 * <ul>
 * <li>{@link #partitionExpressions} - define how input data must be partitioned.
 *     Each must be a variable reference</li>
 * <li>{@link #orderExpressions} - define how data inside these partitions must be ordered.
 *     Each must be a variable reference</li>
 * <li>{@link #frameValueExpressions} - value expressions for comparing against frame start / end boundaries and frame
 *     exclusion. Each must be a variable reference</li>
 * <li>{@link #frameStartExpressions} - frame start boundary</li>
 * <li>{@link #frameStartValidationExpressions} - frame start boundary validators</li>
 * <li>{@link #frameEndExpressions} - frame end boundary</li>
 * <li>{@link #frameEndValidationExpressions} - frame end boundary validators</li>
 * <li>{@link #frameExcludeExpressions} and {@link #frameExcludeUnaryExpression} - define values to be excluded from
 *     the frame</li>
 * <li>{@link #frameOffsetExpression} - sets how many tuples to skip inside each frame after exclusion is applied</li>
 * <li>{@link #frameMaxObjects} - limits number of tuples to be returned for each frame
 * ({@link #FRAME_MAX_OBJECTS_UNLIMITED} = unlimited)</li>
 * <li>{@link #variables} - output variables containing return values of these functions</li>
 * <li>{@link #expressions} - window function expressions (running aggregates)</li>
 * </ul>
 *
 * Window operator does not change cardinality of the input stream.
 */
public class WindowOperator extends AbstractOperatorWithNestedPlans {

    public static final int FRAME_MAX_OBJECTS_UNLIMITED = -1;

    private final List<Mutable<ILogicalExpression>> partitionExpressions;

    private final List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions;

    private final List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> frameValueExpressions;

    private final List<Mutable<ILogicalExpression>> frameStartExpressions;

    private final List<Mutable<ILogicalExpression>> frameStartValidationExpressions;

    private final List<Mutable<ILogicalExpression>> frameEndExpressions;

    private final List<Mutable<ILogicalExpression>> frameEndValidationExpressions;

    private final List<Mutable<ILogicalExpression>> frameExcludeExpressions;

    private int frameExcludeNegationStartIdx;

    private final Mutable<ILogicalExpression> frameExcludeUnaryExpression;

    private final Mutable<ILogicalExpression> frameOffsetExpression;

    private int frameMaxObjects;

    private final List<LogicalVariable> variables;

    private final List<Mutable<ILogicalExpression>> expressions;

    public WindowOperator(List<Mutable<ILogicalExpression>> partitionExpressions,
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions) {
        this(partitionExpressions, orderExpressions, null, null, null, null, null, null, -1, null, null, -1);
    }

    public WindowOperator(List<Mutable<ILogicalExpression>> partitionExpressions,
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions,
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> frameValueExpressions,
            List<Mutable<ILogicalExpression>> frameStartExpressions,
            List<Mutable<ILogicalExpression>> frameStartValidationExpressions,
            List<Mutable<ILogicalExpression>> frameEndExpressions,
            List<Mutable<ILogicalExpression>> frameEndValidationExpressions,
            List<Mutable<ILogicalExpression>> frameExcludeExpressions, int frameExcludeNegationStartIdx,
            ILogicalExpression frameExcludeUnaryExpression, ILogicalExpression frameOffsetExpression,
            int frameMaxObjects) {
        this.partitionExpressions = new ArrayList<>();
        if (partitionExpressions != null) {
            this.partitionExpressions.addAll(partitionExpressions);
        }
        this.orderExpressions = new ArrayList<>();
        if (orderExpressions != null) {
            this.orderExpressions.addAll(orderExpressions);
        }
        this.frameValueExpressions = new ArrayList<>();
        if (frameValueExpressions != null) {
            this.frameValueExpressions.addAll(frameValueExpressions);
        }
        this.frameStartExpressions = new ArrayList<>();
        if (frameStartExpressions != null) {
            this.frameStartExpressions.addAll(frameStartExpressions);
        }
        this.frameStartValidationExpressions = new ArrayList<>();
        if (frameStartValidationExpressions != null) {
            this.frameStartValidationExpressions.addAll(frameStartValidationExpressions);
        }
        this.frameEndExpressions = new ArrayList<>();
        if (frameEndExpressions != null) {
            this.frameEndExpressions.addAll(frameEndExpressions);
        }
        this.frameEndValidationExpressions = new ArrayList<>();
        if (frameEndValidationExpressions != null) {
            this.frameEndValidationExpressions.addAll(frameEndValidationExpressions);
        }
        this.frameExcludeExpressions = new ArrayList<>();
        if (frameExcludeExpressions != null) {
            this.frameExcludeExpressions.addAll(frameExcludeExpressions);
        }
        this.frameExcludeNegationStartIdx = frameExcludeNegationStartIdx;
        this.frameExcludeUnaryExpression = new MutableObject<>(frameExcludeUnaryExpression);
        this.frameOffsetExpression = new MutableObject<>(frameOffsetExpression);
        this.variables = new ArrayList<>();
        this.expressions = new ArrayList<>();
        setFrameMaxObjects(frameMaxObjects);
    }

    public WindowOperator(List<Mutable<ILogicalExpression>> partitionExpressions,
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExpressions,
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> frameValueExpressions,
            List<Mutable<ILogicalExpression>> frameStartExpressions,
            List<Mutable<ILogicalExpression>> frameStartValidationExpressions,
            List<Mutable<ILogicalExpression>> frameEndExpressions,
            List<Mutable<ILogicalExpression>> frameEndValidationExpressions,
            List<Mutable<ILogicalExpression>> frameExcludeExpressions, int frameExcludeNegationStartIdx,
            ILogicalExpression frameExcludeUnaryExpression, ILogicalExpression frameOffsetExpression,
            int frameMaxObjects, List<LogicalVariable> variables, List<Mutable<ILogicalExpression>> expressions,
            List<ILogicalPlan> nestedPlans) {
        this(partitionExpressions, orderExpressions, frameValueExpressions, frameStartExpressions,
                frameStartValidationExpressions, frameEndExpressions, frameEndValidationExpressions,
                frameExcludeExpressions, frameExcludeNegationStartIdx, frameExcludeUnaryExpression,
                frameOffsetExpression, frameMaxObjects);
        if (variables != null) {
            this.variables.addAll(variables);
        }
        if (expressions != null) {
            this.expressions.addAll(expressions);
        }
        if (nestedPlans != null) {
            this.nestedPlans.addAll(nestedPlans);
        }
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.WINDOW;
    }

    public List<Mutable<ILogicalExpression>> getPartitionExpressions() {
        return partitionExpressions;
    }

    public List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> getOrderExpressions() {
        return orderExpressions;
    }

    public List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> getFrameValueExpressions() {
        return frameValueExpressions;
    }

    public List<Mutable<ILogicalExpression>> getFrameStartExpressions() {
        return frameStartExpressions;
    }

    public List<Mutable<ILogicalExpression>> getFrameStartValidationExpressions() {
        return frameStartValidationExpressions;
    }

    public List<Mutable<ILogicalExpression>> getFrameEndExpressions() {
        return frameEndExpressions;
    }

    public List<Mutable<ILogicalExpression>> getFrameEndValidationExpressions() {
        return frameEndValidationExpressions;
    }

    public List<Mutable<ILogicalExpression>> getFrameExcludeExpressions() {
        return frameExcludeExpressions;
    }

    public int getFrameExcludeNegationStartIdx() {
        return frameExcludeNegationStartIdx;
    }

    public void setFrameExcludeNegationStartIdx(int value) {
        this.frameExcludeNegationStartIdx = value;
    }

    public Mutable<ILogicalExpression> getFrameExcludeUnaryExpression() {
        return frameExcludeUnaryExpression;
    }

    public Mutable<ILogicalExpression> getFrameOffsetExpression() {
        return frameOffsetExpression;
    }

    public int getFrameMaxObjects() {
        return frameMaxObjects;
    }

    public void setFrameMaxObjects(int value) {
        frameMaxObjects = value < 0 ? FRAME_MAX_OBJECTS_UNLIMITED : value;
    }

    public List<LogicalVariable> getVariables() {
        return variables;
    }

    public List<Mutable<ILogicalExpression>> getExpressions() {
        return expressions;
    }

    @Override
    public boolean hasNestedPlans() {
        return !nestedPlans.isEmpty();
    }

    @Override
    public void recomputeSchema() {
        super.recomputeSchema();
        schema.addAll(variables);
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitWindowOperator(this, arg);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return acceptExpressionTransform(visitor, true);
    }

    /**
     * Allows performing expression transformation only on a subset of this operator's expressions
     *
     * @param visitor
     *            transforming visitor
     * @param visitVarRefRequiringExprs
     *            whether to visit variable reference requiring expressions, or not
     */
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor,
            boolean visitVarRefRequiringExprs) throws AlgebricksException {
        boolean mod = false;
        if (visitVarRefRequiringExprs) {
            for (Mutable<ILogicalExpression> expr : partitionExpressions) {
                mod |= visitor.transform(expr);
            }
            for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : orderExpressions) {
                mod |= visitor.transform(p.second);
            }
            for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : frameValueExpressions) {
                mod |= visitor.transform(p.second);
            }
        }
        for (Mutable<ILogicalExpression> expr : frameStartExpressions) {
            mod |= visitor.transform(expr);
        }
        for (Mutable<ILogicalExpression> expr : frameStartValidationExpressions) {
            mod |= visitor.transform(expr);
        }
        for (Mutable<ILogicalExpression> expr : frameEndExpressions) {
            mod |= visitor.transform(expr);
        }
        for (Mutable<ILogicalExpression> expr : frameEndValidationExpressions) {
            mod |= visitor.transform(expr);
        }
        for (Mutable<ILogicalExpression> excludeExpr : frameExcludeExpressions) {
            mod |= visitor.transform(excludeExpr);
        }
        if (frameExcludeUnaryExpression.getValue() != null) {
            mod |= visitor.transform(frameExcludeUnaryExpression);
        }
        if (frameOffsetExpression.getValue() != null) {
            mod |= visitor.transform(frameOffsetExpression);
        }
        for (Mutable<ILogicalExpression> expr : expressions) {
            mod |= visitor.transform(expr);
        }
        return mod;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources) {
                target.addAllVariables(sources[0]);
                for (LogicalVariable v : variables) {
                    target.addVariable(v);
                }
            }
        };
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = createNestedPlansPropagatingTypeEnvironment(ctx, true);
        for (int i = 0, n = variables.size(); i < n; i++) {
            env.setVarType(variables.get(i), ctx.getExpressionTypeComputer().getType(expressions.get(i).getValue(),
                    ctx.getMetadataProvider(), env));
        }
        return env;
    }

    @Override
    public void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        vars.addAll(variables);
    }

    @Override
    public void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        for (Mutable<ILogicalExpression> expr : partitionExpressions) {
            expr.getValue().getUsedVariables(vars);
        }
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : orderExpressions) {
            p.second.getValue().getUsedVariables(vars);
        }
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : frameValueExpressions) {
            p.second.getValue().getUsedVariables(vars);
        }
        for (Mutable<ILogicalExpression> expr : frameStartExpressions) {
            expr.getValue().getUsedVariables(vars);
        }
        for (Mutable<ILogicalExpression> expr : frameStartValidationExpressions) {
            expr.getValue().getUsedVariables(vars);
        }
        for (Mutable<ILogicalExpression> expr : frameEndExpressions) {
            expr.getValue().getUsedVariables(vars);
        }
        for (Mutable<ILogicalExpression> expr : frameEndValidationExpressions) {
            expr.getValue().getUsedVariables(vars);
        }
        for (Mutable<ILogicalExpression> excludeExpr : frameExcludeExpressions) {
            excludeExpr.getValue().getUsedVariables(vars);
        }
        if (frameExcludeUnaryExpression.getValue() != null) {
            frameExcludeUnaryExpression.getValue().getUsedVariables(vars);
        }
        if (frameOffsetExpression.getValue() != null) {
            frameOffsetExpression.getValue().getUsedVariables(vars);
        }
        for (Mutable<ILogicalExpression> expr : expressions) {
            expr.getValue().getUsedVariables(vars);
        }
    }

    /**
     * Only the following expressions require variable references: {@link #partitionExpressions},
     * {@link #orderExpressions}, and {@link #frameValueExpressions}, others do not.
     * Use {@link #acceptExpressionTransform(ILogicalExpressionReferenceTransform, boolean)}
     * to visit only non-requiring expressions.
     */
    @Override
    public boolean requiresVariableReferenceExpressions() {
        return false;
    }

    public List<LogicalVariable> getPartitionVarList() {
        List<LogicalVariable> varList = new ArrayList<>(partitionExpressions.size());
        for (Mutable<ILogicalExpression> pe : partitionExpressions) {
            ILogicalExpression partExpr = pe.getValue();
            if (partExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                LogicalVariable var = ((VariableReferenceExpression) partExpr).getVariableReference();
                varList.add(var);
            }
        }
        return varList;
    }

    public List<OrderColumn> getOrderColumnList() {
        List<OrderColumn> orderColumns = new ArrayList<>(orderExpressions.size());
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : orderExpressions) {
            ILogicalExpression orderExpr = p.second.getValue();
            if (orderExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                LogicalVariable var = ((VariableReferenceExpression) orderExpr).getVariableReference();
                orderColumns.add(new OrderColumn(var, p.first.getKind()));
            }
        }
        return orderColumns;
    }
}
