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

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.rewriter.rules.PushMapOperatorThroughUnionRule;

public class AsterixPushMapOperatorThroughUnionRule extends PushMapOperatorThroughUnionRule {

    private final FieldAccessByIndexCollector fieldAccessByIndexCollector = new FieldAccessByIndexCollector();

    private final FieldAccessByIndexTransformer fieldAccessByIndexTransformer = new FieldAccessByIndexTransformer();

    private final Set<LogicalOperatorTag> allowedKinds;

    public AsterixPushMapOperatorThroughUnionRule(LogicalOperatorTag... allowedKinds) {
        if (allowedKinds.length == 0) {
            throw new IllegalArgumentException();
        }
        this.allowedKinds = EnumSet.noneOf(LogicalOperatorTag.class);
        Collections.addAll(this.allowedKinds, allowedKinds);
    }

    @Override
    protected boolean isOperatorKindPushableThroughUnion(ILogicalOperator op) {
        //TODO(dmitry): support subplan operator
        return allowedKinds.contains(op.getOperatorTag()) && super.isOperatorKindPushableThroughUnion(op);
    }

    @Override
    protected Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> deepCopyForBranch(ILogicalOperator op,
            Set<LogicalVariable> opUsedVars, UnionAllOperator unionAllOp, int branchIdx, IOptimizationContext context)
            throws AlgebricksException {

        if (((AbstractLogicalOperator) op).hasNestedPlans()) {
            //TODO(dmitry): support subplan operator
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, op.getSourceLocation(),
                    op.getOperatorTag().toString());
        }

        fieldAccessByIndexCollector.reset(unionAllOp, branchIdx, context);
        op.acceptExpressionTransform(fieldAccessByIndexCollector);
        if (fieldAccessByIndexCollector.failed) {
            fieldAccessByIndexCollector.clear();
            return null;
        }

        Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> newOpPair =
                super.deepCopyForBranch(op, opUsedVars, unionAllOp, branchIdx, context);

        if (fieldAccessByIndexCollector.hasFieldAccessMappings()) {
            fieldAccessByIndexTransformer.reset(unionAllOp, branchIdx, context);
            newOpPair.first.acceptExpressionTransform(fieldAccessByIndexTransformer);
            fieldAccessByIndexTransformer.clear();
        }

        fieldAccessByIndexCollector.clear();
        return newOpPair;
    }

    private static final class FieldAccessByIndexCollector extends AbstractFieldAccessByIndexTransformer {

        private final Map<Pair<LogicalVariable, Integer>, Integer> fieldIndexMap = new HashMap<>();

        private boolean failed;

        @Override
        void reset(UnionAllOperator unionAllOp, int branchIdx, IOptimizationContext context) {
            super.reset(unionAllOp, branchIdx, context);
            fieldIndexMap.clear();
            failed = false;
        }

        @Override
        void clear() {
            super.clear();
            fieldIndexMap.clear();
        }

        boolean hasFieldAccessMappings() {
            return !fieldIndexMap.isEmpty();
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            visit(exprRef);
            return false;
        }

        private void visit(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return;
            }
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
            for (Mutable<ILogicalExpression> argExpr : callExpr.getArguments()) {
                visit(argExpr);
                if (failed) {
                    return;
                }
            }

            if (callExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
                boolean mapped = mapFieldIndex(callExpr);
                if (!mapped) {
                    failed = true;
                }
            }
        }

        private boolean mapFieldIndex(AbstractFunctionCallExpression callExpr) throws AlgebricksException {
            // the record variable in the field access should match the output variable from union, i.e. $2.getField
            ILogicalExpression recordExpr = callExpr.getArguments().get(0).getValue();
            if (recordExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return false;
            }
            Integer fieldIndexPostUnion = ConstantExpressionUtil.getIntArgument(callExpr, 1);
            if (fieldIndexPostUnion == null) {
                return false;
            }
            LogicalVariable recordVarPostUnion = ((VariableReferenceExpression) recordExpr).getVariableReference();
            // 'recordVarPostUnion' is a post-union var, we need to find corresponding pre-union var
            for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMap : unionAllOp.getVariableMappings()) {
                if (varMap.third.equals(recordVarPostUnion)) {
                    LogicalVariable recordVarPreUnion = branchIdx == 0 ? varMap.first : varMap.second;

                    IVariableTypeEnvironment typeEnvPostUnion = context.getOutputTypeEnvironment(unionAllOp);
                    ARecordType recordTypePostUnion = (ARecordType) typeEnvPostUnion.getVarType(recordVarPostUnion);
                    String fieldName = recordTypePostUnion.getFieldNames()[fieldIndexPostUnion];

                    ILogicalOperator inputOpToUnion = unionAllOp.getInputs().get(branchIdx).getValue();
                    IVariableTypeEnvironment typeEnvPreUnion = context.getOutputTypeEnvironment(inputOpToUnion);
                    ARecordType recordTypePreUnion = (ARecordType) typeEnvPreUnion.getVarType(recordVarPreUnion);

                    int fieldIndexPreUnion = recordTypePreUnion.getFieldIndex(fieldName);
                    if (fieldIndexPreUnion >= 0) {
                        // we save 'recordVar' pre-union because super.deepCopyForBranch() will replace
                        // post-union variables with pre-union variables in the operator's expressions
                        fieldIndexMap.put(new Pair<>(recordVarPreUnion, fieldIndexPostUnion), fieldIndexPreUnion);
                        return true;
                    } else {
                        return false;
                    }
                }

            }
            return false;
        }

    }

    private final class FieldAccessByIndexTransformer extends AbstractFieldAccessByIndexTransformer {

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return false;
            }
            boolean applied = false;
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
            for (Mutable<ILogicalExpression> argument : callExpr.getArguments()) {
                applied |= transform(argument);
            }

            if (callExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
                transformFieldIndex(callExpr);
                applied = true;
            }
            return applied;
        }

        private void transformFieldIndex(AbstractFunctionCallExpression callExpr) throws AlgebricksException {
            ILogicalExpression recordExpr = callExpr.getArguments().get(0).getValue();
            if (recordExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, callExpr.getSourceLocation(),
                        recordExpr.getExpressionTag().toString());
            }
            Integer fieldIndexPostUnion = ConstantExpressionUtil.getIntArgument(callExpr, 1);
            if (fieldIndexPostUnion == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, callExpr.getSourceLocation(), "");
            }
            // this 'recordVar' is pre-union because super.deepCopyForBranch() replaced
            // post-union variables with pre-union variables in the operator's expressions
            LogicalVariable recordVarPreUnion = ((VariableReferenceExpression) recordExpr).getVariableReference();
            Integer fieldIndexPreUnion =
                    fieldAccessByIndexCollector.fieldIndexMap.get(new Pair<>(recordVarPreUnion, fieldIndexPostUnion));
            if (fieldIndexPreUnion == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, callExpr.getSourceLocation(),
                        recordVarPreUnion.toString());
            }
            callExpr.getArguments().get(1)
                    .setValue(new ConstantExpression(new AsterixConstantValue(new AInt32(fieldIndexPreUnion))));
        }
    }

    private abstract static class AbstractFieldAccessByIndexTransformer
            implements ILogicalExpressionReferenceTransform {

        protected UnionAllOperator unionAllOp;

        protected int branchIdx;

        protected IOptimizationContext context;

        void reset(UnionAllOperator unionAllOp, int branchIdx, IOptimizationContext context) {
            this.unionAllOp = unionAllOp;
            this.branchIdx = branchIdx;
            this.context = context;
        }

        void clear() {
            unionAllOp = null;
            context = null;
        }
    }
}
