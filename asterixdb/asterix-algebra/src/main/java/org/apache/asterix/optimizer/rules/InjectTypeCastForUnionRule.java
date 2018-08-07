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

package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.dataflow.data.common.TypeResolverUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * This rule injects type casts for inputs of a UnionAllOperator if those
 * inputs have heterogeneous types.
 */
public class InjectTypeCastForUnionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
            return false;
        }
        UnionAllOperator unionAllOperator = (UnionAllOperator) op;
        // Injects casts to the first and second input branch of the UnionAllOperator.
        return injectCast(unionAllOperator, 0, context) || injectCast(unionAllOperator, 1, context);
    }

    // Injects a type cast function on one input (indicated by childIndex) of the union all operator if necessary.
    private boolean injectCast(UnionAllOperator op, int childIndex, IOptimizationContext context)
            throws AlgebricksException {
        // Gets the type environments for the union all operator and its child operator with the right child index.
        IVariableTypeEnvironment env = context.getOutputTypeEnvironment(op);
        Mutable<ILogicalOperator> branchOpRef = op.getInputs().get(childIndex);
        ILogicalOperator branchOp = branchOpRef.getValue();
        IVariableTypeEnvironment childEnv = context.getOutputTypeEnvironment(branchOp);
        SourceLocation sourceLoc = branchOp.getSourceLocation();

        // The two lists are used for the assign operator that calls cast functions.
        List<LogicalVariable> varsToCast = new ArrayList<>();
        List<Mutable<ILogicalExpression>> castFunctionsForLeft = new ArrayList<>();

        // Iterate through all triples.
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> triples = op.getVariableMappings();
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> triple : triples) {
            LogicalVariable producedVar = triple.third;
            IAType producedType = (IAType) env.getVarType(producedVar);
            LogicalVariable varToCast = childIndex == 0 ? triple.first : triple.second;
            IAType inputType = (IAType) childEnv.getVarType(varToCast);
            if (!TypeResolverUtil.needsCast(producedType, inputType)) {
                // Continues to the next triple if no cast is neeeded.
                continue;
            }
            LogicalVariable castedVar = context.newVar();
            // Resets triple variables to new variables that bind to the results of type casting.
            triple.first = childIndex == 0 ? castedVar : triple.first;
            triple.second = childIndex > 0 ? castedVar : triple.second;
            VariableReferenceExpression varToCastRef = new VariableReferenceExpression(varToCast);
            varToCastRef.setSourceLocation(sourceLoc);
            ScalarFunctionCallExpression castFunc =
                    new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CAST_TYPE),
                            new ArrayList<>(Collections.singletonList(new MutableObject<>(varToCastRef))));
            castFunc.setSourceLocation(sourceLoc);
            TypeCastUtils.setRequiredAndInputTypes(castFunc, producedType, inputType);

            // Adds the variable and function expression into lists, for the assign operator.
            varsToCast.add(castedVar);
            castFunctionsForLeft.add(new MutableObject<>(castFunc));
        }
        if (castFunctionsForLeft.isEmpty()) {
            return false;
        }
        // Injects an assign operator to perform type casts.
        AssignOperator assignOp = new AssignOperator(varsToCast, castFunctionsForLeft);
        assignOp.setSourceLocation(sourceLoc);
        assignOp.setExecutionMode(branchOp.getExecutionMode());
        assignOp.getInputs().add(new MutableObject<>(branchOp));
        branchOpRef.setValue(assignOp);
        context.computeAndSetTypeEnvironmentForOperator(assignOp);

        // Returns true to indicate that rewriting happens.
        return true;
    }

}
