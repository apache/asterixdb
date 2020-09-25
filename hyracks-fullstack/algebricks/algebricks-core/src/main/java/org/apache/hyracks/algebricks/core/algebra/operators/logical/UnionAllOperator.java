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
    private final List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap;

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
        // Assume input schemas are
        // input0 = [a,b,c]
        // input1 = [d,e,f,g,h]
        // and
        // UNION ALL mapping is
        // [a,d -> X], [c,f -> Y]
        //
        // In order to compute the output schema we need to pick a larger input
        // out of these two and replace variables there using UNION ALL mappings.
        // Therefore in this example we'll pick input1 and the output schema will be
        // [X,e,Y,g,h]
        //
        // Note that all input variables are out of scope after UNION ALL
        // therefore it's ok return them in the output schema because
        // no parent operator will refer to them.
        // Also note the all UNION ALL operators in the final optimized plan
        // will have input schemas that exactly match their mappings.
        // This is guaranteed by InsertProjectBeforeUnionRule.
        // In this example in the final optimized plan
        // input0 schema will be [a,c]
        // input1 schema will be [d,f]

        List<LogicalVariable> inputSchema0 = inputs.get(0).getValue().getSchema();
        List<LogicalVariable> inputSchema1 = inputs.get(1).getValue().getSchema();

        List<LogicalVariable> inputSchema;
        int inputSchemaIdx;
        if (inputSchema0.size() >= inputSchema1.size()) {
            inputSchema = inputSchema0;
            inputSchemaIdx = 0;
        } else {
            inputSchema = inputSchema1;
            inputSchemaIdx = 1;
        }

        schema = new ArrayList<>(inputSchema);
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> t : varMap) {
            LogicalVariable inVar = inputSchemaIdx == 0 ? t.first : t.second;
            LogicalVariable outVar = t.third;
            boolean mappingFound = false;
            for (int i = 0, n = schema.size(); i < n; i++) {
                if (schema.get(i).equals(inVar)) {
                    schema.set(i, outVar);
                    mappingFound = true;
                }
            }
            if (!mappingFound) {
                schema.add(outVar);
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
