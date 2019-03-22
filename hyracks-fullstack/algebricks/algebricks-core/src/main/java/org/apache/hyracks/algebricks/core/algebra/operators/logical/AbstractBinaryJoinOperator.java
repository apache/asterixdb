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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

public abstract class AbstractBinaryJoinOperator extends AbstractLogicalOperator {
    protected final Mutable<ILogicalExpression> condition;
    protected JoinKind joinKind;

    public enum JoinKind {
        INNER,
        LEFT_OUTER
    }

    public AbstractBinaryJoinOperator(JoinKind joinKind, Mutable<ILogicalExpression> condition) {
        this.joinKind = joinKind;
        this.condition = condition;
    }

    public AbstractBinaryJoinOperator(JoinKind joinKind, Mutable<ILogicalExpression> condition,
            Mutable<ILogicalOperator> input1, Mutable<ILogicalOperator> input2) {
        this(joinKind, condition);
        inputs.add(input1);
        inputs.add(input2);
    }

    public Mutable<ILogicalExpression> getCondition() {
        return condition;
    }

    public JoinKind getJoinKind() {
        return joinKind;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(inputs.get(0).getValue().getSchema());
        schema.addAll(inputs.get(1).getValue().getSchema());
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.concat(VariablePropagationPolicy.ALL, VariablePropagationPolicy.ALL);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return visitor.transform(condition);
    }

    @Override
    public boolean isMap() {
        return false;
    }
}
