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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;

/**
 * Adds equivalent classes for record-constructors.
 * For example, for $x:=record-constructor("field1": $v, "field2": $t),
 * two equivalent classes will be added:
 * <$v, field-access-by-index($x, 0)>
 * <$t, field-access-by-index($x, 1)>
 *
 * @author yingyi
 */
public class AddEquivalenceClassForRecordConstructorRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        // Computes FDs and equivalence classes for the operator.
        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(op, context);
        AssignOperator assignOp = (AssignOperator) op;
        List<LogicalVariable> vars = assignOp.getVariables();
        List<Mutable<ILogicalExpression>> exprRefs = assignOp.getExpressions();
        return addEquivalenceClassesForRecordConstructor(vars, exprRefs, assignOp, context);
    }

    private boolean addEquivalenceClassesForRecordConstructor(List<LogicalVariable> vars,
            List<Mutable<ILogicalExpression>> exprRefs, AssignOperator assignOp, IOptimizationContext context) {
        boolean changed = false;
        for (int exprIndex = 0; exprIndex < exprRefs.size(); ++exprIndex) {
            ILogicalExpression expr = exprRefs.get(exprIndex).getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
                FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
                if (fid == BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR
                        || fid == BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR) {
                    changed |= propagateEquivalenceClassesForRecordConstructor(vars.get(exprIndex), funcExpr, assignOp,
                            context);
                }
            }
        }
        return changed;
    }

    @SuppressWarnings("unchecked")
    private boolean propagateEquivalenceClassesForRecordConstructor(LogicalVariable recordVar,
            AbstractFunctionCallExpression funcExpr, AssignOperator assignOp, IOptimizationContext context) {
        List<Mutable<ILogicalExpression>> argRefs = funcExpr.getArguments();
        boolean changed = false;
        // Only odd position arguments are field value expressions.
        for (int parameterIndex = 1; parameterIndex < argRefs.size(); parameterIndex += 2) {
            ILogicalExpression fieldExpr = argRefs.get(parameterIndex).getValue();
            // Adds equivalent classes if a field is from a variable reference.
            if (fieldExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) fieldExpr;
                LogicalVariable fieldVar = varExpr.getVariableReference();
                Map<LogicalVariable, EquivalenceClass> ecs = context.getEquivalenceClassMap(assignOp);
                if (ecs == null) {
                    ecs = new HashMap<>();
                    context.putEquivalenceClassMap(assignOp, ecs);
                }
                VariableReferenceExpression recordVarRef = new VariableReferenceExpression(recordVar);
                recordVarRef.setSourceLocation(funcExpr.getSourceLocation());
                ScalarFunctionCallExpression expr = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                        new MutableObject<>(recordVarRef), new MutableObject<>(
                                new ConstantExpression(new AsterixConstantValue(new AInt32(parameterIndex / 2))))); // Every two parameters corresponds to a field.
                expr.setSourceLocation(funcExpr.getSourceLocation());
                EquivalenceClass equivClass = new EquivalenceClass(Collections.singletonList(fieldVar), fieldVar,
                        Collections.singletonList(expr));
                ecs.put(fieldVar, equivClass);
                changed = true;
            }
        }
        return changed;
    }

}
