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
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.OpRefTypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class SelectOperator extends AbstractLogicalOperator {
    private final Mutable<ILogicalExpression> condition;
    private final IAlgebricksConstantValue retainMissingAsValue;
    private LogicalVariable missingPlaceholderVar;

    public SelectOperator(Mutable<ILogicalExpression> condition) {
        this(condition, null, null);
    }

    public SelectOperator(Mutable<ILogicalExpression> condition, IAlgebricksConstantValue retainMissingAsValue,
            LogicalVariable missingPlaceholderVar) {
        this.condition = condition;
        if (retainMissingAsValue == null) {
            this.retainMissingAsValue = null;
            if (missingPlaceholderVar != null) {
                throw new IllegalArgumentException(missingPlaceholderVar.toString());
            }
        } else if (retainMissingAsValue.isMissing()) {
            this.retainMissingAsValue = ConstantExpression.MISSING.getValue();
        } else if (retainMissingAsValue.isNull()) {
            this.retainMissingAsValue = ConstantExpression.NULL.getValue();
        } else {
            throw new IllegalArgumentException(retainMissingAsValue.toString());
        }
        this.missingPlaceholderVar = missingPlaceholderVar;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.SELECT;
    }

    public Mutable<ILogicalExpression> getCondition() {
        return condition;
    }

    public IAlgebricksConstantValue getRetainMissingAsValue() {
        return retainMissingAsValue;
    }

    public LogicalVariable getMissingPlaceholderVariable() {
        return missingPlaceholderVar;
    }

    public void setMissingPlaceholderVar(LogicalVariable var) {
        if (var != null && retainMissingAsValue == null) {
            throw new IllegalArgumentException("NULL/MISSING var " + var + " is set, but its value not specified");
        }
        missingPlaceholderVar = var;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<>(inputs.get(0).getValue().getSchema());
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return visitor.transform(condition);
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitSelectOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return true;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        ITypeEnvPointer[] envPointers = new ITypeEnvPointer[1];
        envPointers[0] = new OpRefTypeEnvPointer(inputs.get(0), ctx);
        PropagatingTypeEnvironment env = new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(),
                ctx.getMissableTypeComputer(), ctx.getMetadataProvider(), TypePropagationPolicy.ALL, envPointers);
        if (condition.getValue().getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return env;
        }
        AbstractFunctionCallExpression f1 = (AbstractFunctionCallExpression) condition.getValue();
        if (!f1.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.NOT)) {
            return env;
        }
        ILogicalExpression a1 = f1.getArguments().get(0).getValue();
        if (a1.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression f2 = (AbstractFunctionCallExpression) a1;
            FunctionIdentifier f2id = f2.getFunctionIdentifier();
            if (f2id.equals(AlgebricksBuiltinFunctions.IS_MISSING)) {
                extractFunctionArgVarInto(f2, env.getNonMissableVariables());
            } else if (f2id.equals(AlgebricksBuiltinFunctions.IS_NULL)) {
                extractFunctionArgVarInto(f2, env.getNonNullableVariables());
            }
        }
        return env;
    }

    @Override
    public boolean requiresVariableReferenceExpressions() {
        return false;
    }

    private static void extractFunctionArgVarInto(AbstractFunctionCallExpression callExpr,
            List<? super LogicalVariable> outList) {
        ILogicalExpression arg = callExpr.getArguments().get(0).getValue();
        if (arg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            LogicalVariable var = ((VariableReferenceExpression) arg).getVariableReference();
            outList.add(var);
        }
    }
}
