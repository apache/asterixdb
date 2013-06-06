/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.OpRefTypeEnvPointer;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class SelectOperator extends AbstractLogicalOperator {
    private final Mutable<ILogicalExpression> condition;

    public SelectOperator(Mutable<ILogicalExpression> condition) {
        this.condition = condition;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.SELECT;
    }

    public Mutable<ILogicalExpression> getCondition() {
        return condition;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>(inputs.get(0).getValue().getSchema());
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
                ctx.getNullableTypeComputer(), ctx.getMetadataProvider(), TypePropagationPolicy.ALL, envPointers);
        if (condition.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression f1 = (AbstractFunctionCallExpression) condition.getValue();
            if (f1.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.NOT)) {
                ILogicalExpression a1 = f1.getArguments().get(0).getValue();
                if (a1.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression f2 = (AbstractFunctionCallExpression) a1;
                    if (f2.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.IS_NULL)) {
                        ILogicalExpression a2 = f2.getArguments().get(0).getValue();
                        if (a2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                            LogicalVariable var = ((VariableReferenceExpression) a2).getVariableReference();
                            env.getNonNullVariables().add(var);
                        }
                    }
                }
            }
        }
        return env;
    }

    @Override
    public boolean requiresVariableReferenceExpressions() {
        return false;
    }
}