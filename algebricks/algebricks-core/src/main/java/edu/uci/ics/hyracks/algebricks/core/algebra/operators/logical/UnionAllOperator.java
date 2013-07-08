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
import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.NonPropagatingTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class UnionAllOperator extends AbstractLogicalOperator {

    // (left-var, right-var, out-var)
    private List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap;

    public UnionAllOperator(List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap) {
        this.varMap = varMap;
    }

    public List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> getVariableMappings() {
        return varMap;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.UNIONALL;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {

            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> t : varMap) {
                    target.addVariable(t.third);
                }
            }
        };

    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitUnionOperator(this, arg);
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        for (LogicalVariable v1 : inputs.get(0).getValue().getSchema()) {
            for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> t : varMap) {
                if (t.first.equals(v1)) {
                    schema.add(t.third);
                } else {
                    schema.add(v1);
                }
            }
        }
        for (LogicalVariable v2 : inputs.get(1).getValue().getSchema()) {
            for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> t : varMap) {
                if (t.second.equals(v2)) {
                    schema.add(t.third);
                } else {
                    schema.add(v2);
                }
            }
        }
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(),
                ctx.getMetadataProvider());
        IVariableTypeEnvironment envLeft = ctx.getOutputTypeEnvironment(inputs.get(0).getValue());
        if (envLeft == null) {
            throw new AlgebricksException("Left input types for union operator are not computed.");
        }
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> t : varMap) {
            Object t1 = envLeft.getVarType(t.first);
            if (t1 == null) {
                throw new AlgebricksException("Failed typing union operator: no type for variable " + t.first);
            }
            env.setVarType(t.third, t1);
        }
        return env;
    }

}
