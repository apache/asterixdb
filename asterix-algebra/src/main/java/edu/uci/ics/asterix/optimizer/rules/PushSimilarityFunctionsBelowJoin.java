/*
 * Copyright 2009-2012 by The Regents of the University of California
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Pushes similarity function-call expressions below a join if possible.
 * Assigns the similarity function-call expressions to new variables, and replaces the original
 * expression with a corresponding variable reference expression.
 * This rule can help reduce the cost of computing expensive similarity functions by pushing them below
 * a join (which may blow up the cardinality).
 * Also, this rule may help to enable other rules such as common subexpression elimination, again to reduce
 * the number of calls to expensive similarity functions.
 * 
 * Example:
 * 
 * Before plan:
 * assign [$$10] <- [funcA(funcB(simFuncX($$3, $$4)))]
 *   join (some condition) 
 *     join_branch_0 where $$3 and $$4 are not live
 *       ...
 *     join_branch_1 where $$3 and $$4 are live
 *       ...
 * 
 * After plan:
 * assign [$$10] <- [funcA(funcB($$11))]
 *   join (some condition) 
 *     join_branch_0 where $$3 and $$4 are not live
 *       ...
 *     join_branch_1 where $$3 and $$4 are live
 *       assign[$$11] <- [simFuncX($$3, $$4)]
 *         ...
 */
public class PushSimilarityFunctionsBelowJoin implements IAlgebraicRewriteRule {

    private static final Set<FunctionIdentifier> simFuncIdents = new HashSet<FunctionIdentifier>();
    static {
        simFuncIdents.add(AsterixBuiltinFunctions.SIMILARITY_JACCARD);
        simFuncIdents.add(AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK);
        simFuncIdents.add(AsterixBuiltinFunctions.EDIT_DISTANCE);
        simFuncIdents.add(AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK);
    }

    private final List<Mutable<ILogicalExpression>> simFuncExprs = new ArrayList<Mutable<ILogicalExpression>>();
    private final List<LogicalVariable> usedVars = new ArrayList<LogicalVariable>();
    private final List<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assignOp = (AssignOperator) op;

        // Find a join operator below this assign.
        Mutable<ILogicalOperator> joinOpRef = findJoinOp(assignOp.getInputs().get(0));
        if (joinOpRef == null) {
            return false;
        }
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinOpRef.getValue();

        // Check if the assign uses a similarity function that we wish to push below the join if possible.
        simFuncExprs.clear();
        gatherSimilarityFunctionCalls(assignOp, simFuncExprs);
        if (simFuncExprs.isEmpty()) {
            return false;
        }

        // Try to push the similarity functions down the input branches of the join.
        boolean modified = false;
        if (pushDownSimilarityFunctions(joinOp, 0, simFuncExprs, context)) {
            modified = true;
        }
        if (pushDownSimilarityFunctions(joinOp, 1, simFuncExprs, context)) {
            modified = true;
        }
        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(joinOp);
        }
        return modified;
    }

    private Mutable<ILogicalOperator> findJoinOp(Mutable<ILogicalOperator> opRef) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        switch (op.getOperatorTag()) {
            case INNERJOIN:
            case LEFTOUTERJOIN: {
                return opRef;
            }
            // Bail on these operators.
            case GROUP:
            case AGGREGATE:
            case DISTINCT:
            case UNNEST_MAP: {
                return null;
            }
            // Traverse children.
            default: {
                for (Mutable<ILogicalOperator> childOpRef : op.getInputs()) {
                    return findJoinOp(childOpRef);
                }
            }
        }
        return null;
    }

    private void gatherSimilarityFunctionCalls(AssignOperator assignOp, List<Mutable<ILogicalExpression>> simFuncExprs) {
        for (Mutable<ILogicalExpression> exprRef : assignOp.getExpressions()) {
            gatherSimilarityFunctionCalls(exprRef, simFuncExprs);
        }
    }

    private void gatherSimilarityFunctionCalls(Mutable<ILogicalExpression> exprRef,
            List<Mutable<ILogicalExpression>> simFuncExprs) {
        AbstractLogicalExpression expr = (AbstractLogicalExpression) exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }
        // Check whether the function is a similarity function.
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (simFuncIdents.contains(funcExpr.getFunctionIdentifier())) {
            simFuncExprs.add(exprRef);
        }
        // Traverse arguments.
        for (Mutable<ILogicalExpression> funcArg : funcExpr.getArguments()) {
            gatherSimilarityFunctionCalls(funcArg, simFuncExprs);
        }
    }

    private boolean pushDownSimilarityFunctions(AbstractBinaryJoinOperator joinOp, int inputIndex,
            List<Mutable<ILogicalExpression>> simFuncExprs, IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator joinInputOp = joinOp.getInputs().get(inputIndex).getValue();
        liveVars.clear();
        VariableUtilities.getLiveVariables(joinInputOp, liveVars);
        Iterator<Mutable<ILogicalExpression>> simFuncIter = simFuncExprs.iterator();
        List<LogicalVariable> assignVars = null;
        List<Mutable<ILogicalExpression>> assignExprs = null;
        while (simFuncIter.hasNext()) {
            Mutable<ILogicalExpression> simFuncExprRef = simFuncIter.next();
            ILogicalExpression simFuncExpr = simFuncExprRef.getValue();
            usedVars.clear();
            simFuncExpr.getUsedVariables(usedVars);
            // Check if we can push the similarity function down this branch.
            if (liveVars.containsAll(usedVars)) {
                if (assignVars == null) {
                    assignVars = new ArrayList<LogicalVariable>();
                    assignExprs = new ArrayList<Mutable<ILogicalExpression>>();
                }
                // Replace the original expression with a variable reference expression.
                LogicalVariable replacementVar = context.newVar();
                assignVars.add(replacementVar);
                assignExprs.add(new MutableObject<ILogicalExpression>(simFuncExpr));
                simFuncExprRef.setValue(new VariableReferenceExpression(replacementVar));
                simFuncIter.remove();
            }
        }
        // Create new assign operator below the join if any similarity functions can be pushed.
        if (assignVars != null) {
            AssignOperator newAssign = new AssignOperator(assignVars, assignExprs);
            newAssign.getInputs().add(new MutableObject<ILogicalOperator>(joinInputOp));
            newAssign.setExecutionMode(joinOp.getExecutionMode());
            joinOp.getInputs().get(inputIndex).setValue(newAssign);
            context.computeAndSetTypeEnvironmentForOperator(newAssign);
            return true;
        }
        return false;
    }
}
