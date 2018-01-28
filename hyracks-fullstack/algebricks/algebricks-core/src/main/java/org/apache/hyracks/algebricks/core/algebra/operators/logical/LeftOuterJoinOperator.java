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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.OpRefTypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class LeftOuterJoinOperator extends AbstractBinaryJoinOperator {

    public LeftOuterJoinOperator(Mutable<ILogicalExpression> condition) {
        super(JoinKind.LEFT_OUTER, condition);
    }

    public LeftOuterJoinOperator(Mutable<ILogicalExpression> condition, Mutable<ILogicalOperator> input1,
            Mutable<ILogicalOperator> input2) {
        super(JoinKind.LEFT_OUTER, condition, input1, input2);
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.LEFTOUTERJOIN;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitLeftOuterJoinOperator(this, arg);
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        int n = inputs.size();
        ITypeEnvPointer[] envPointers = new ITypeEnvPointer[n];
        for (int i = 0; i < n; i++) {
            envPointers[i] = new OpRefTypeEnvPointer(inputs.get(i), ctx);
        }
        PropagatingTypeEnvironment env =
                new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMissableTypeComputer(),
                        ctx.getMetadataProvider(), TypePropagationPolicy.LEFT_OUTER, envPointers);
        List<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(inputs.get(1).getValue(), liveVars); // live variables from outer branch can be null together
        env.getCorrelatedMissableVariableLists().add(liveVars);
        return env;
    }

}
