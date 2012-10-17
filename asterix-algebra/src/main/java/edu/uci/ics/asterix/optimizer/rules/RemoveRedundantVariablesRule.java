/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Replaces redundant variable references with their bottom-most equivalent representative.
 * Does a DFS sweep over the plan keeping track of variable equivalence classes.
 * For example, this rule would perform the following rewrite.
 * 
 * Before Plan:
 * select (function-call: func, Args:[%0->$$11])
 *   project [$11]
 *     assign [$$11] <- [$$10]
 *       assign [$$10] <- [$$9]
 *         assign [$$9] <- ...
 *           ...
 *           
 * After Plan:
 * select (function-call: func, Args:[%0->$$9])
 *   project [$9]
 *     assign [$$11] <- [$$9]
 *       assign [$$10] <- [$$9]
 *         assign [$$9] <- ...
 *           ...
 */
public class RemoveRedundantVariablesRule implements IAlgebraicRewriteRule {

    private final VariableSubstitutionVisitor substVisitor = new VariableSubstitutionVisitor();
    // TODO: Rename to equivalentVarsMap
    private final Map<LogicalVariable, List<LogicalVariable>> equivalenceClasses = new HashMap<LogicalVariable, List<LogicalVariable>>();
    
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        boolean modified = removeRedundantVariables(opRef, context);
        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
        }
        return modified;
    }

    private void updateEquivalenceClassMap(LogicalVariable lhs, LogicalVariable rhs) {
        List<LogicalVariable> equivalentVars = equivalenceClasses.get(rhs);
        if (equivalentVars == null) {
            equivalentVars = new ArrayList<LogicalVariable>();
            // The first element in the list is the bottom-most representative which will replace all equivalent vars.
            equivalentVars.add(rhs);
            equivalentVars.add(lhs);
            equivalenceClasses.put(rhs, equivalentVars);
        }
        equivalenceClasses.put(lhs, equivalentVars);
        equivalentVars.get(0);
    }
    
    private boolean removeRedundantVariables(Mutable<ILogicalOperator> opRef,
            IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();        
        boolean modified = false;
        
        // Recurse into children.
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            if (removeRedundantVariables(inputOpRef, context)) {
                modified = true;
            }
        }

        // Update equivalence classes and replace variable references with their first representative.
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {            
            AssignOperator assignOp = (AssignOperator) op;
            int numVars = assignOp.getVariables().size();
            for (int i = 0; i < numVars; i++) {
                ILogicalExpression expr = assignOp.getExpressions().get(i).getValue();
                if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                    continue;
                }
                VariableReferenceExpression rhsVarRefExpr = (VariableReferenceExpression) expr;
                // Update equivalence class map.
                LogicalVariable lhs = assignOp.getVariables().get(i);
                LogicalVariable rhs = rhsVarRefExpr.getVariableReference();
                updateEquivalenceClassMap(lhs, rhs);
            }
        }
        
        switch (op.getOperatorTag()) {
            case PROJECT: {
                // The project operator does not use expressions, so we need to replace it's variables manually.
                if (replaceProjectVars((ProjectOperator) op)) {
                    modified = true;
                }
                break;
            }
            default: {
                if (op.acceptExpressionTransform(substVisitor)) {
                    modified = true;
                    context.computeAndSetTypeEnvironmentForOperator(op);
                }
            }
        }
        

        /*
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans n = (AbstractOperatorWithNestedPlans) op;
            List<EquivalenceClass> eqc = equivClasses;
            if (n.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                eqc = new LinkedList<EquivalenceClass>();
            } else {
                eqc = equivClasses;
            }
            for (ILogicalPlan p : n.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    Pair<Boolean, Boolean> bb = collectEqClassesAndRemoveRedundantOps(r, context, false, eqc,
                            substVisitor, substVisitorForWrites);
                    if (bb.first) {
                        modified = true;
                    }
                    if (bb.second) {
                        ecChange = true;
                    }
                }
            }
        }
        // we assume a variable is assigned a value only once
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator a = (AssignOperator) op;
            ILogicalExpression rhs = a.getExpressions().get(0).getValue();
            if (rhs.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                LogicalVariable varLeft = a.getVariables().get(0);
                VariableReferenceExpression varRef = (VariableReferenceExpression) rhs;
                LogicalVariable varRight = varRef.getVariableReference();

                EquivalenceClass ecRight = findEquivClass(varRight, equivClasses);
                if (ecRight != null) {
                    ecRight.addMember(varLeft);
                } else {
                    List<LogicalVariable> m = new LinkedList<LogicalVariable>();
                    m.add(varRight);
                    m.add(varLeft);
                    EquivalenceClass ec = new EquivalenceClass(m, varRight);
                    equivClasses.add(ec);
                    if (AlgebricksConfig.DEBUG) {
                        AlgebricksConfig.ALGEBRICKS_LOGGER.finest("--- New equivalence class: " + ec + "\n");
                    }
                }
                ecChange = true;
            } else if (((AbstractLogicalExpression) rhs).getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                LogicalVariable varLeft = a.getVariables().get(0);
                List<LogicalVariable> m = new LinkedList<LogicalVariable>();
                m.add(varLeft);
                EquivalenceClass ec = new EquivalenceClass(m, (ConstantExpression) rhs);
                // equivClassesForParent.add(ec);
                equivClasses.add(ec);
                ecChange = true;
            }
        } else if (op.getOperatorTag() == LogicalOperatorTag.GROUP && !(context.checkIfInDontApplySet(this, op))) {
            GroupByOperator group = (GroupByOperator) op;
            Pair<Boolean, Boolean> r1 = processVarExprPairs(group.getGroupByList(), equivClasses);
            Pair<Boolean, Boolean> r2 = processVarExprPairs(group.getDecorList(), equivClasses);
            modified = modified || r1.first || r2.first;
            ecChange = r1.second || r2.second;
        }
        if (op.getOperatorTag() == LogicalOperatorTag.PROJECT) {
            assignVarsNeededByProject((ProjectOperator) op, equivClasses, context);
        } else {
            boolean assignRecord = false;
            if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) op;
                List<Mutable<ILogicalExpression>> exprRefs = assignOp.getExpressions();
                for (Mutable<ILogicalExpression> exprRef : exprRefs) {
                    ILogicalExpression expr = exprRef.getValue();
                    if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        ScalarFunctionCallExpression funExpr = (ScalarFunctionCallExpression) expr;
                        if (funExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)
                                || funExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR)) {
                            assignRecord = true;
                            break;
                        }

                    }
                }
            }

            if (op.getOperatorTag() == LogicalOperatorTag.WRITE
                    || op.getOperatorTag() == LogicalOperatorTag.INSERT_DELETE
                    || op.getOperatorTag() == LogicalOperatorTag.INDEX_INSERT_DELETE
                    || op.getOperatorTag() == LogicalOperatorTag.WRITE_RESULT || assignRecord) {
                substVisitorForWrites.setEquivalenceClasses(equivClasses);
                if (op.acceptExpressionTransform(substVisitorForWrites)) {
                    modified = true;
                }
            } else {
                substVisitor.setEquivalenceClasses(equivClasses);
                if (op.acceptExpressionTransform(substVisitor)) {
                    modified = true;
                    if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
                        GroupByOperator group = (GroupByOperator) op;
                        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> gp : group.getGroupByList()) {
                            if (gp.first != null
                                    && gp.second.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                                LogicalVariable gv = ((VariableReferenceExpression) gp.second.getValue())
                                        .getVariableReference();
                                Iterator<Pair<LogicalVariable, Mutable<ILogicalExpression>>> iter = group.getDecorList()
                                        .iterator();
                                while (iter.hasNext()) {
                                    Pair<LogicalVariable, Mutable<ILogicalExpression>> dp = iter.next();
                                    if (dp.first == null
                                            && dp.second.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                                        LogicalVariable dv = ((VariableReferenceExpression) dp.second.getValue())
                                                .getVariableReference();
                                        if (dv == gv) {
                                            // The decor variable is redundant,
                                            // since it is
                                            // propagated as a grouping
                                            // variable.
                                            EquivalenceClass ec1 = findEquivClass(gv, equivClasses);
                                            if (ec1 != null) {
                                                ec1.addMember(gp.first);
                                                ec1.setVariableRepresentative(gp.first);
                                            } else {
                                                List<LogicalVariable> varList = new ArrayList<LogicalVariable>();
                                                varList.add(gp.first);
                                                varList.add(gv);
                                                ec1 = new EquivalenceClass(varList, gp.first);
                                            }
                                            iter.remove();
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(op);
        }
        return new Pair<Boolean, Boolean>(modified, ecChange);
        */
        return modified;
    }

    private boolean replaceProjectVars(ProjectOperator op) throws AlgebricksException {
        List<LogicalVariable> vars = op.getVariables();
        int size = vars.size();
        boolean modified = false;
        for (int i = 0; i < size; i++) {
            LogicalVariable var = vars.get(i);
            List<LogicalVariable> equivalentVars = equivalenceClasses.get(var);
            if (equivalentVars == null) {
                continue;
            }
            // Replace with representative.
            LogicalVariable representative = equivalentVars.get(0);
            if (representative != var) {
                vars.set(i, equivalentVars.get(0));
                modified = true;
            }
        }
        return modified;
    }
    
    private class VariableSubstitutionVisitor implements ILogicalExpressionReferenceTransform {
        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) {
            ILogicalExpression e = exprRef.getValue();
            switch (((AbstractLogicalExpression) e).getExpressionTag()) {
                case VARIABLE: {
                    // Replace variable references with their equivalent representative in the equivalence class map.
                    VariableReferenceExpression varRefExpr = (VariableReferenceExpression) e;
                    LogicalVariable var = varRefExpr.getVariableReference();
                    List<LogicalVariable> equivalentVars = equivalenceClasses.get(var);
                    if (equivalentVars == null) {
                        return false;
                    }
                    LogicalVariable representative = equivalentVars.get(0);
                    if (representative != var) {
                        varRefExpr.setVariable(representative);
                        return true;
                    }
                    return false;
                }
                case FUNCTION_CALL: {
                    AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) e;
                    boolean modified = false;
                    for (Mutable<ILogicalExpression> arg : fce.getArguments()) {
                        if (transform(arg)) {
                            modified = true;
                        }
                    }
                    return modified;
                }
                default: {
                    return false;
                }
            }
        }
    }
}
