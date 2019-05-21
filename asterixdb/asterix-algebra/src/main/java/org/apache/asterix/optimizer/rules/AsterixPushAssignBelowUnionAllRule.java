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

import java.util.List;

import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.rewriter.rules.PushAssignBelowUnionAllRule;

public class AsterixPushAssignBelowUnionAllRule extends PushAssignBelowUnionAllRule {

    // modifies field-access-by-index by adjusting the index to match the one in the branch where assign is moved to
    @Override
    protected boolean modifyExpression(ILogicalExpression expression, UnionAllOperator unionOp,
            IOptimizationContext ctx, int inputIndex) throws AlgebricksException {
        if (expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return true; // expressions other than functions need not be modified
        }
        AbstractFunctionCallExpression functionCall = (AbstractFunctionCallExpression) expression;
        List<Mutable<ILogicalExpression>> arguments = functionCall.getArguments();
        for (int k = 0, size = arguments.size(); k < size; k++) {
            if (!modifyExpression(arguments.get(k).getValue(), unionOp, ctx, inputIndex)) {
                return false;
            }
        }
        // return true if any function other than field-access-by-index. otherwise, try to map the index.
        return !functionCall.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)
                || mapFieldIndex(functionCall, unionOp, ctx, inputIndex);
    }

    private static boolean mapFieldIndex(AbstractFunctionCallExpression functionCall, UnionAllOperator unionOp,
            IOptimizationContext ctx, int inputIndex) throws AlgebricksException {
        // the record variable in the field access should match the output variable from union, i.e. $2.getField
        ILogicalExpression recordExpr = functionCall.getArguments().get(0).getValue();
        if (recordExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        Integer fieldIndex = ConstantExpressionUtil.getIntArgument(functionCall, 1);
        if (fieldIndex == null) {
            return false;
        }
        LogicalVariable recordVar = ((VariableReferenceExpression) recordExpr).getVariableReference();
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMap : unionOp.getVariableMappings()) {
            if (varMap.third.equals(recordVar)) {
                ARecordType unionType = (ARecordType) ctx.getOutputTypeEnvironment(unionOp).getVarType(varMap.third);
                ILogicalOperator inputOpToUnion = unionOp.getInputs().get(inputIndex).getValue();
                ARecordType inputType;
                if (inputIndex == 0) {
                    inputType = (ARecordType) ctx.getOutputTypeEnvironment(inputOpToUnion).getVarType(varMap.first);
                } else {
                    inputType = (ARecordType) ctx.getOutputTypeEnvironment(inputOpToUnion).getVarType(varMap.second);
                }
                String fieldName = unionType.getFieldNames()[fieldIndex];
                fieldIndex = inputType.getFieldIndex(fieldName);
                if (fieldIndex < 0) {
                    return false;
                }
                functionCall.getArguments().get(1)
                        .setValue(new ConstantExpression(new AsterixConstantValue(new AInt32(fieldIndex))));
                return true;
            }
        }
        return false;
    }
}
