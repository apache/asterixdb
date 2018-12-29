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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Factors out common sub-expressions by assigning them to a variables, and replacing the common sub-expressions with references to those variables.
 * Preconditions/Assumptions:
 * Assumes no projects are in the plan. This rule ignores variable reference expressions and constants (other rules deal with those separately).
 * Postconditions/Examples:
 * Plan with extracted sub-expressions. Generates one assign operator per extracted expression.
 * Example 1 - Simple Arithmetic Example (simplified)
 * Before plan:
 * assign [$$1] <- [5 + 6 - 10]
 * assign [$$0] <- [5 + 6 + 30]
 * After plan:
 * assign [$$1] <- [$$5 - 10]
 * assign [$$0] <- [$$5 + 30]
 * assign [$$5] <- [5 + 6]
 * Example 2 - Cleaning up 'Distinct By' (simplified)
 * Before plan: (notice how $$0 is not live after the distinct)
 * assign [$$3] <- [field-access($$0, 1)]
 * distinct ([%0->$$5])
 * assign [$$5] <- [field-access($$0, 1)]
 * unnest $$0 <- [scan-dataset]
 * After plan: (notice how the issue of $$0 is fixed)
 * assign [$$3] <- [$$5]
 * distinct ([$$5])
 * assign [$$5] <- [field-access($$0, 1)]
 * unnest $$0 <- [scan-dataset]
 */
public class ExtractCommonExpressionsRule implements IAlgebraicRewriteRule {

    private final List<ILogicalExpression> originalAssignExprs = new ArrayList<ILogicalExpression>();

    private final CommonExpressionSubstitutionVisitor substVisitor = new CommonExpressionSubstitutionVisitor();
    private final Map<ILogicalExpression, ExprEquivalenceClass> exprEqClassMap =
            new HashMap<ILogicalExpression, ExprEquivalenceClass>();

    // Set of operators for which common subexpression elimination should not be performed.
    private static final Set<LogicalOperatorTag> ignoreOps = new HashSet<LogicalOperatorTag>(6);

    static {
        ignoreOps.add(LogicalOperatorTag.UNNEST);
        ignoreOps.add(LogicalOperatorTag.UNNEST_MAP);
        ignoreOps.add(LogicalOperatorTag.ORDER);
        ignoreOps.add(LogicalOperatorTag.PROJECT);
        ignoreOps.add(LogicalOperatorTag.AGGREGATE);
        ignoreOps.add(LogicalOperatorTag.RUNNINGAGGREGATE);
        ignoreOps.add(LogicalOperatorTag.WINDOW); //TODO: can extract from partition/order/frame expressions
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        exprEqClassMap.clear();
        substVisitor.setContext(context);
        boolean modified = removeCommonExpressions(opRef, context);
        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
        }
        return modified;
    }

    private void updateEquivalenceClassMap(LogicalVariable lhs, Mutable<ILogicalExpression> rhsExprRef,
            ILogicalExpression rhsExpr, ILogicalOperator op) {
        ExprEquivalenceClass exprEqClass = exprEqClassMap.get(rhsExpr);
        if (exprEqClass == null) {
            exprEqClass = new ExprEquivalenceClass(op, rhsExprRef);
            exprEqClassMap.put(rhsExpr, exprEqClass);
        }
        exprEqClass.setVariable(lhs);
    }

    private boolean removeCommonExpressions(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }

        boolean modified = false;
        // Recurse into children.
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            if (removeCommonExpressions(inputOpRef, context)) {
                modified = true;
            }
        }

        // TODO: Deal with replicate properly. Currently, we just clear the expr equivalence map,
        // since we want to avoid incorrect expression replacement
        // (the resulting new variables should be assigned live below a replicate/split).
        if (op.getOperatorTag() == LogicalOperatorTag.REPLICATE || op.getOperatorTag() == LogicalOperatorTag.SPLIT) {
            exprEqClassMap.clear();
            return modified;
        }
        // Exclude these operators.
        if (ignoreOps.contains(op.getOperatorTag())) {
            return modified;
        }

        // Remember a copy of the original assign expressions, so we can add them to the equivalence class map
        // after replacing expressions within the assign operator itself.
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) op;
            originalAssignExprs.clear();
            int numVars = assignOp.getVariables().size();
            for (int i = 0; i < numVars; i++) {
                Mutable<ILogicalExpression> exprRef = assignOp.getExpressions().get(i);
                ILogicalExpression expr = exprRef.getValue();
                originalAssignExprs.add(expr.cloneExpression());
            }
        }

        // Perform common subexpression elimination.
        substVisitor.setOperator(op);
        if (op.acceptExpressionTransform(substVisitor)) {
            modified = true;
        }

        // Update equivalence class map.
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) op;
            int numVars = assignOp.getVariables().size();
            for (int i = 0; i < numVars; i++) {
                Mutable<ILogicalExpression> exprRef = assignOp.getExpressions().get(i);
                ILogicalExpression expr = exprRef.getValue();
                if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE
                        || expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    continue;
                }
                // Update equivalence class map.
                LogicalVariable lhs = assignOp.getVariables().get(i);
                updateEquivalenceClassMap(lhs, exprRef, exprRef.getValue(), op);

                // Update equivalence class map with original assign expression.
                updateEquivalenceClassMap(lhs, exprRef, originalAssignExprs.get(i), op);
            }
        }

        // TODO: For now do not perform replacement in nested plans
        // due to the complication of figuring out whether the firstOp in an equivalence class is within a subplan,
        // and the resulting variable will not be visible to the outside.
        // Since subplans should be eliminated in most cases, this behavior is acceptable for now.
        /*
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opWithNestedPlan = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan nestedPlan : opWithNestedPlan.getNestedPlans()) {
                for (Mutable<ILogicalOperator> rootRef : nestedPlan.getRoots()) {
                    if (removeCommonExpressions(rootRef, context)) {
                        modified = true;
                    }
                }
            }
        }
        */

        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(op);
            context.addToDontApplySet(this, op);
        }
        return modified;
    }

    private class CommonExpressionSubstitutionVisitor implements ILogicalExpressionReferenceTransform {

        private IOptimizationContext context;
        private ILogicalOperator op;

        public void setContext(IOptimizationContext context) {
            this.context = context;
        }

        public void setOperator(ILogicalOperator op) throws AlgebricksException {
            this.op = op;
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            AbstractLogicalExpression expr = (AbstractLogicalExpression) exprRef.getValue();
            boolean modified = false;
            ExprEquivalenceClass exprEqClass = exprEqClassMap.get(expr);
            if (exprEqClass != null) {
                // Replace common subexpression with existing variable.
                if (exprEqClass.variableIsSet()) {
                    if (expr.isFunctional()) {
                        Set<LogicalVariable> liveVars = new HashSet<>();
                        List<LogicalVariable> usedVars = new ArrayList<>();
                        VariableUtilities.getLiveVariables(op, liveVars);
                        VariableUtilities.getUsedVariables(op, usedVars);
                        // Check if the replacing variable is live at this op.
                        // However, if the op is already using variables that are not live,
                        // then a replacement may enable fixing the plan.
                        // This behavior is necessary to, e.g., properly deal with distinct by.
                        // Also just replace the expr if we are replacing common exprs from within the same operator.
                        if (liveVars.contains(exprEqClass.getVariable()) || !liveVars.containsAll(usedVars)
                                || op == exprEqClass.getFirstOperator()) {
                            VariableReferenceExpression varRef =
                                    new VariableReferenceExpression(exprEqClass.getVariable());
                            varRef.setSourceLocation(expr.getSourceLocation());
                            exprRef.setValue(varRef);
                            // Do not descend into children since this expr has been completely replaced.
                            return true;
                        }
                    }
                } else {
                    if (expr.isFunctional() && assignCommonExpression(exprEqClass, expr)) {
                        //re-obtain the live vars after rewriting in the method called in the if condition
                        Set<LogicalVariable> liveVars = new HashSet<LogicalVariable>();
                        VariableUtilities.getLiveVariables(op, liveVars);
                        //rewrite only when the variable is live
                        if (liveVars.contains(exprEqClass.getVariable())) {
                            VariableReferenceExpression varRef =
                                    new VariableReferenceExpression(exprEqClass.getVariable());
                            varRef.setSourceLocation(expr.getSourceLocation());
                            exprRef.setValue(varRef);
                            // Do not descend into children since this expr has been completely replaced.
                            return true;
                        }
                    }
                }
            } else {
                if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE
                        && expr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                    exprEqClass = new ExprEquivalenceClass(op, exprRef);
                    exprEqClassMap.put(expr, exprEqClass);
                }
            }

            // Descend into function arguments.
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
                for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                    if (transform(arg)) {
                        modified = true;
                    }
                }
            }
            return modified;
        }

        private boolean assignCommonExpression(ExprEquivalenceClass exprEqClass, ILogicalExpression expr)
                throws AlgebricksException {
            SourceLocation sourceLoc = expr.getSourceLocation();
            AbstractLogicalOperator firstOp = (AbstractLogicalOperator) exprEqClass.getFirstOperator();
            Mutable<ILogicalExpression> firstExprRef = exprEqClass.getFirstExpression();
            // We don't consider to eliminate common exprs in join operators by doing a cartesian production
            // and pulling the condition in to a select. This will negatively impact the performance.
            if (firstOp.getInputs().size() > 1) {
                // Bail for any non-join operator with multiple inputs.
                return false;
            }
            LogicalVariable newVar = context.newVar();
            AssignOperator newAssign = new AssignOperator(newVar,
                    new MutableObject<ILogicalExpression>(firstExprRef.getValue().cloneExpression()));
            newAssign.setSourceLocation(sourceLoc);
            // Place assign below firstOp.
            newAssign.getInputs().add(new MutableObject<ILogicalOperator>(firstOp.getInputs().get(0).getValue()));
            newAssign.setExecutionMode(firstOp.getExecutionMode());
            firstOp.getInputs().get(0).setValue(newAssign);
            // Replace original expr with variable reference, and set var in expression equivalence class.
            VariableReferenceExpression newVarRef = new VariableReferenceExpression(newVar);
            newVarRef.setSourceLocation(sourceLoc);
            firstExprRef.setValue(newVarRef);
            exprEqClass.setVariable(newVar);
            context.computeAndSetTypeEnvironmentForOperator(newAssign);
            context.computeAndSetTypeEnvironmentForOperator(firstOp);
            return true;
        }

        private ILogicalExpression getEnclosingExpression(Mutable<ILogicalExpression> conditionExprRef,
                ILogicalExpression commonSubExpr) {
            ILogicalExpression conditionExpr = conditionExprRef.getValue();
            if (conditionExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return null;
            }
            if (isEqJoinCondition(commonSubExpr)) {
                // Do not eliminate the common expression if we could use it for an equi-join.
                return null;
            }
            AbstractFunctionCallExpression conditionFuncExpr = (AbstractFunctionCallExpression) conditionExpr;
            // Boolean expression that encloses the common subexpression.
            ILogicalExpression enclosingBoolExpr = null;
            // We are not dealing with arbitrarily nested and/or expressions here.
            FunctionIdentifier funcIdent = conditionFuncExpr.getFunctionIdentifier();
            if (funcIdent.equals(AlgebricksBuiltinFunctions.AND) || funcIdent.equals(AlgebricksBuiltinFunctions.OR)) {
                Iterator<Mutable<ILogicalExpression>> argIter = conditionFuncExpr.getArguments().iterator();
                while (argIter.hasNext()) {
                    Mutable<ILogicalExpression> argRef = argIter.next();
                    if (containsExpr(argRef.getValue(), commonSubExpr)) {
                        enclosingBoolExpr = argRef.getValue();
                        // Remove the enclosing expression from the argument list.
                        // We are going to pull it out into a new select operator.
                        argIter.remove();
                        break;
                    }
                }
                // If and/or only has a single argument left, pull it out and remove the and/or function.
                if (conditionFuncExpr.getArguments().size() == 1) {
                    conditionExprRef.setValue(conditionFuncExpr.getArguments().get(0).getValue());
                }
            } else {
                if (!containsExpr(conditionExprRef.getValue(), commonSubExpr)) {
                    return null;
                }
                enclosingBoolExpr = conditionFuncExpr;
                // Replace the enclosing expression with TRUE.
                conditionExprRef.setValue(ConstantExpression.TRUE);
            }
            return enclosingBoolExpr;
        }
    }

    private boolean containsExpr(ILogicalExpression expr, ILogicalExpression searchExpr) {
        if (expr == searchExpr) {
            return true;
        }
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            if (containsExpr(argRef.getValue(), searchExpr)) {
                return true;
            }
        }
        return false;
    }

    private boolean isEqJoinCondition(ILogicalExpression expr) {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (funcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.EQ)) {
            ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
            ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
            if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE
                    && arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                return true;
            }
        }
        return false;
    }

    private final class ExprEquivalenceClass {
        // First operator in which expression is used.
        private final ILogicalOperator firstOp;

        // Reference to expression in first op.
        private final Mutable<ILogicalExpression> firstExprRef;

        // Variable that this expression has been assigned to.
        private LogicalVariable var;

        public ExprEquivalenceClass(ILogicalOperator firstOp, Mutable<ILogicalExpression> firstExprRef) {
            this.firstOp = firstOp;
            this.firstExprRef = firstExprRef;
        }

        public ILogicalOperator getFirstOperator() {
            return firstOp;
        }

        public Mutable<ILogicalExpression> getFirstExpression() {
            return firstExprRef;
        }

        public void setVariable(LogicalVariable var) {
            this.var = var;
        }

        public LogicalVariable getVariable() {
            return var;
        }

        public boolean variableIsSet() {
            return var != null;
        }
    }
}
