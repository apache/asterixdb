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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.NonPropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

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
        IVariableTypeEnvironment env =
                new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMetadataProvider());
        IVariableTypeEnvironment envLeft = ctx.getOutputTypeEnvironment(inputs.get(0).getValue());
        IVariableTypeEnvironment envRight = ctx.getOutputTypeEnvironment(inputs.get(1).getValue());
        if (envLeft == null) {
            throw new AlgebricksException("Left input types for union operator are not computed.");
        }
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> t : varMap) {
            Object typeFromLeft = getType(envLeft, t.first);
            Object typeFromRight = getType(envRight, t.second);
            if (typeFromLeft.equals(typeFromRight)) {
                env.setVarType(t.third, typeFromLeft);
            } else {
                env.setVarType(t.third, ctx.getConflictingTypeResolver().resolve(typeFromLeft, typeFromRight));
            }
        }
        return env;
    }

    // Gets the type of a variable from an type environment.
    private Object getType(IVariableTypeEnvironment env, LogicalVariable var) throws AlgebricksException {
        Object type = env.getVarType(var);
        if (type == null) {
            throw new AlgebricksException("Failed typing union operator: no type for variable " + var);
        }
        return type;
    }
}
