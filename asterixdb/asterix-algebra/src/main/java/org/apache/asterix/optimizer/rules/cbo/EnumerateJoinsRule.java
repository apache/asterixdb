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

package org.apache.asterix.optimizer.rules.cbo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.annotations.IndexedNLJoinExpressionAnnotation;
import org.apache.asterix.metadata.entities.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.HashJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EnumerateJoinsRule implements IAlgebraicRewriteRule {

    private static final Logger LOGGER = LogManager.getLogger();

    protected final JoinEnum joinEnum;

    public EnumerateJoinsRule(JoinEnum joinEnum) {
        this.joinEnum = joinEnum;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    /**
     * If this method returns false, that means CBO code will not be used to optimize the part of the join graph that
     * was passed in. Currently, we do not optimize query graphs with outer joins in them. If the CBO code is activated
     * a new join graph (with inputs possibly switched) will be created and the return value will be true.
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean cboMode = this.getCBOMode(context);
        boolean cboTestMode = this.getCBOTestMode(context);
        if (!(cboMode || cboTestMode)) {
            return false;
        }
        // If we reach here, then either cboMode or cboTestMode is true.
        // If cboTestMode is true, then we use predefined cardinalities for datasets for asterixdb regression tests.
        // If cboMode is true, then all datasets need to have samples, otherwise the check in doAllDataSourcesHaveSamples()
        // further below will return false.
        ILogicalOperator op = opRef.getValue();
        if (!((op.getOperatorTag() == LogicalOperatorTag.INNERJOIN)
                || ((op.getOperatorTag() == LogicalOperatorTag.DISTRIBUTE_RESULT)))) {
            return false;
        }

        // if this join has already been seen before, no need to apply the rule again
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        List<ILogicalOperator> joinOps = new ArrayList<>();
        List<ILogicalOperator> internalEdges = new ArrayList<>();
        HashMap<EmptyTupleSourceOperator, ILogicalOperator> joinLeafInputsHashMap = new HashMap<>();
        // The data scan operators. Will be in the order of the from clause.
        // Important for position ordering when assigning bits to join expressions.
        List<Pair<EmptyTupleSourceOperator, DataSourceScanOperator>> emptyTupleAndDataSourceOps = new ArrayList<>();
        HashMap<DataSourceScanOperator, EmptyTupleSourceOperator> dataSourceEmptyTupleHashMap = new HashMap<>();

        IPlanPrettyPrinter pp = context.getPrettyPrinter();
        printPlan(pp, (AbstractLogicalOperator) op, "Original Whole plan1");
        boolean canTransform = getJoinOpsAndLeafInputs(op, emptyTupleAndDataSourceOps, joinLeafInputsHashMap,
                dataSourceEmptyTupleHashMap, internalEdges, joinOps);

        if (!canTransform) {
            return false;
        }

        // if this happens, something in the input plan is not acceptable to the new code.
        if (emptyTupleAndDataSourceOps.size() != joinLeafInputsHashMap.size()) {
            throw new IllegalStateException(
                    "ETS " + emptyTupleAndDataSourceOps.size() + " != LI " + joinLeafInputsHashMap.size());
        }

        printPlan(pp, (AbstractLogicalOperator) op, "Original Whole plan2");

        int numberOfFromTerms = emptyTupleAndDataSourceOps.size();

        joinEnum.initEnum((AbstractLogicalOperator) op, cboMode, cboTestMode, numberOfFromTerms,
                emptyTupleAndDataSourceOps, joinLeafInputsHashMap, dataSourceEmptyTupleHashMap, internalEdges, joinOps,
                context);

        if (cboMode) {
            if (!doAllDataSourcesHaveSamples(emptyTupleAndDataSourceOps, context)) {
                return false;
            }
        }

        if (internalEdges.size() > 0) {
            pushAssignsIntoLeafInputs(joinLeafInputsHashMap, internalEdges);
        }

        printPlan(pp, (AbstractLogicalOperator) op, "Original Whole plan3");
        int cheapestPlan = joinEnum.enumerateJoins();
        printPlan(pp, (AbstractLogicalOperator) op, "After join enumeration. Must return same plan??");
        if (cheapestPlan == PlanNode.NO_PLAN) {
            return false;
        }

        PlanNode cheapestPlanNode = joinEnum.allPlans.get(cheapestPlan);

        if (numberOfFromTerms > 1) {
            checkForMultipleUsesOfVariablesInJoinPreds(cheapestPlanNode, joinLeafInputsHashMap, context);
            buildNewTree(cheapestPlanNode, joinLeafInputsHashMap, joinOps, new MutableInt(0));
            printPlan(pp, (AbstractLogicalOperator) joinOps.get(0), "New Whole Plan after buildNewTree 1");
            ILogicalOperator root = addConstantInternalEdgesAtTheTop(joinOps.get(0), internalEdges);
            printPlan(pp, (AbstractLogicalOperator) joinOps.get(0), "New Whole Plan after buildNewTree 2");
            printPlan(pp, (AbstractLogicalOperator) root, "New Whole Plan after buildNewTree");
            // this will be the new root
            opRef.setValue(root);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("---------------------------- Printing Leaf Inputs");
                printLeafPlans(pp, joinLeafInputsHashMap);
                // print joins starting from the bottom
                for (int i = joinOps.size() - 1; i >= 0; i--) {
                    printPlan(pp, (AbstractLogicalOperator) joinOps.get(i), "join " + i);
                }
                printPlan(pp, (AbstractLogicalOperator) joinOps.get(0), "New Whole Plan");
                printPlan(pp, (AbstractLogicalOperator) root, "New Whole Plan");
            }

            // turn of this rule for all joins in this set (subtree)
            for (ILogicalOperator joinOp : joinOps) {
                context.addToDontApplySet(this, joinOp);
            }

        } else {
            buildNewTree(cheapestPlanNode, joinLeafInputsHashMap);
        }

        return true;
    }

    private boolean getCBOMode(IOptimizationContext context) {
        PhysicalOptimizationConfig physOptConfig = context.getPhysicalOptimizationConfig();
        return physOptConfig.getCBOMode();
    }

    private boolean getCBOTestMode(IOptimizationContext context) {
        PhysicalOptimizationConfig physOptConfig = context.getPhysicalOptimizationConfig();
        return physOptConfig.getCBOTestMode();
    }

    /**
     * Should not see any kind of joins here. store the emptyTupeSourceOp and DataSource operators.
     * Each leaf input will normally have both, but sometimes only emptyTupeSourceOp will be present (in lists)
     */
    private Pair<EmptyTupleSourceOperator, DataSourceScanOperator> containsLeafInputOnly(ILogicalOperator op) {
        DataSourceScanOperator dataSourceOp = null;
        ILogicalOperator currentOp = op;
        while (currentOp.getInputs().size() == 1) {
            if (currentOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                // we should not see two data scans in the same path
                if (dataSourceOp != null) {
                    return null;
                }
                dataSourceOp = (DataSourceScanOperator) currentOp;
            }
            currentOp = currentOp.getInputs().get(0).getValue();
        }
        if (currentOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            return new Pair<>((EmptyTupleSourceOperator) currentOp, dataSourceOp);
        }
        return null;
    }

    /**
     * Check to see if there is only one assign here and nothing below that other than a join.
     * have not seen cases where there is more than one assign in a leafinput.
    */
    private boolean onlyOneAssign(ILogicalOperator nextOp) {
        if (nextOp.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        List<Mutable<ILogicalOperator>> nextOpInputs = nextOp.getInputs();
        return nextOpInputs.get(0).getValue().getOperatorTag() == LogicalOperatorTag.INNERJOIN;
    }

    ILogicalOperator findSelect(ILogicalOperator op) {
        LogicalOperatorTag tag;
        while (true) {
            if (op.getInputs().size() > 1) {
                return null; // Assuming only a linear plan for single table queries (as leafInputs are linear).
            }
            tag = op.getOperatorTag();
            if (tag == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                return null; // if this happens, there is nothing we can do in CBO code since there is no datasourcescan
            }
            if (tag == LogicalOperatorTag.SELECT) { // there must be a select operator for CBO to do any optimization.
                return op;
            }

            op = op.getInputs().get(0).getValue();
        }
    }

    /**
     * This is the main routine that stores all the join operators and the leafInputs. We will later reuse the same
     * join operators but switch the leafInputs (see buildNewTree). The whole scheme is based on the assumption that the
     * leafInputs can be switched. The various data structures make the leafInputs accessible efficiently.
     */
    private boolean getJoinOpsAndLeafInputs(ILogicalOperator op,
            List<Pair<EmptyTupleSourceOperator, DataSourceScanOperator>> emptyTupleAndDataSourceOps,
            HashMap<EmptyTupleSourceOperator, ILogicalOperator> joinLeafInputsHashMap,
            HashMap<DataSourceScanOperator, EmptyTupleSourceOperator> dataSourceEmptyTupleHashMap,
            List<ILogicalOperator> internalEdges, List<ILogicalOperator> joinOps) {

        if (op.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }

        if (op.getOperatorTag() == LogicalOperatorTag.INNERJOIN) {
            joinOps.add(op);
            for (int i = 0; i < 2; i++) {
                ILogicalOperator nextOp = op.getInputs().get(i).getValue();
                boolean canTransform = getJoinOpsAndLeafInputs(nextOp, emptyTupleAndDataSourceOps,
                        joinLeafInputsHashMap, dataSourceEmptyTupleHashMap, internalEdges, joinOps);
                if (!canTransform) {
                    return false;
                }
            }
        } else {
            Pair<EmptyTupleSourceOperator, DataSourceScanOperator> etsDataSource = containsLeafInputOnly(op);
            if (etsDataSource != null) { // a leaf input
                EmptyTupleSourceOperator etsOp = etsDataSource.first;
                DataSourceScanOperator dataSourceOp = etsDataSource.second;
                emptyTupleAndDataSourceOps.add(new Pair<>(etsOp, dataSourceOp));
                if (op.getOperatorTag().equals(LogicalOperatorTag.DISTRIBUTE_RESULT)) {// single table query
                    ILogicalOperator selectOp = findSelect(op);
                    if (selectOp == null) {
                        return false;
                    } else {
                        joinLeafInputsHashMap.put(etsOp, selectOp);
                    }
                } else {
                    joinLeafInputsHashMap.put(etsOp, op);
                }
                dataSourceEmptyTupleHashMap.put(dataSourceOp, etsOp);
            } else { // This must be an internal edge
                if (onlyOneAssign(op)) {
                    // currently, will handle only assign statement and nothing else in an internal Edge.
                    // we can lift this restriction later if the need arises. This just makes some code easier.
                    internalEdges.add(op);
                    boolean canTransform =
                            getJoinOpsAndLeafInputs(op.getInputs().get(0).getValue(), emptyTupleAndDataSourceOps,
                                    joinLeafInputsHashMap, dataSourceEmptyTupleHashMap, internalEdges, joinOps);
                    if (!canTransform) {
                        return false;
                    }

                    //internalEdges.add(op); // better to store the parent; do this soon.
                } else {
                    return false;
                }
            }
        }

        return true;
    }

    private void addCardCostAnnotations(ILogicalOperator op, PlanNode plan) {
        op.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_CARDINALITY,
                (double) Math.round(plan.getJoinNode().getCardinality() * 100) / 100);
        op.getAnnotations().put(OperatorAnnotations.OP_COST_TOTAL,
                (double) Math.round(plan.computeTotalCost() * 100) / 100);
        if (plan.IsScanNode()) {
            op.getAnnotations().put(OperatorAnnotations.OP_INPUT_CARDINALITY,
                    (double) Math.round(plan.getJoinNode().getOrigCardinality() * 100) / 100);
            op.getAnnotations().put(OperatorAnnotations.OP_COST_LOCAL,
                    (double) Math.round(plan.computeOpCost() * 100) / 100);
        } else {
            op.getAnnotations().put(OperatorAnnotations.OP_LEFT_EXCHANGE_COST,
                    (double) Math.round(plan.getLeftExchangeCost().computeTotalCost() * 100) / 100);
            op.getAnnotations().put(OperatorAnnotations.OP_RIGHT_EXCHANGE_COST,
                    (double) Math.round(plan.getRightExchangeCost().computeTotalCost() * 100) / 100);
            op.getAnnotations().put(OperatorAnnotations.OP_COST_LOCAL,
                    (double) Math.round((plan.computeOpCost()) * 100) / 100);
        }

        if (op.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
            op.getAnnotations().put(OperatorAnnotations.OP_COST_LOCAL, 0.0);
        }
    }

    /**
     * Finds the DataSourceScanOperator given a leafInput
     */
    private ILogicalOperator findDataSourceScanOperator(ILogicalOperator op) {
        ILogicalOperator origOp = op;
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(LogicalOperatorTag.DATASOURCESCAN)) {
                return op;
            }
            op = op.getInputs().get(0).getValue();
        }
        return origOp;
    }

    private void removeJoinAnnotations(AbstractFunctionCallExpression afcExpr) {
        afcExpr.removeAnnotation(BroadcastExpressionAnnotation.class);
        afcExpr.removeAnnotation(IndexedNLJoinExpressionAnnotation.class);
        afcExpr.removeAnnotation(HashJoinExpressionAnnotation.class);
    }

    private void setAnnotation(AbstractFunctionCallExpression afcExpr, IExpressionAnnotation anno) {
        FunctionIdentifier fi = afcExpr.getFunctionIdentifier();
        List<Mutable<ILogicalExpression>> arguments = afcExpr.getArguments();

        if (fi.equals(AlgebricksBuiltinFunctions.AND)) {
            for (Mutable<ILogicalExpression> iLogicalExpressionMutable : arguments) {
                ILogicalExpression argument = iLogicalExpressionMutable.getValue();
                AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) argument;
                expr.putAnnotation(anno);
            }
        } else {
            afcExpr.putAnnotation(anno);
        }
    }

    //Internal edges are assign statements. The RHS has a variable in it.
    // We need to find the internal edge that has a variable coming from this leaf leafInput.
    private int findInternalEdge(ILogicalOperator leafInput, List<ILogicalOperator> internalEdges)
            throws AlgebricksException {
        int i = -1;

        for (ILogicalOperator ie : internalEdges) {
            i++;
            // this will be an Assign, so no need to check
            AssignOperator aOp = (AssignOperator) ie;
            List<LogicalVariable> vars = new ArrayList<>();
            aOp.getExpressions().get(0).getValue().getUsedVariables(vars);
            HashSet<LogicalVariable> vars2 = new HashSet<>();
            VariableUtilities.getLiveVariables(leafInput, vars2);
            if (vars2.containsAll(vars)) { // note that this will fail if there variables from different leafInputs
                return i;
            }
        }

        return -1;
    }

    private ILogicalOperator addAssignToLeafInput(ILogicalOperator leafInput, ILogicalOperator internalEdge) {
        ILogicalOperator root = leafInput;
        // this will be an Assign, so no need to check
        AssignOperator aOp = (AssignOperator) internalEdge;
        aOp.getInputs().get(0).setValue(root);
        return aOp;
    }

    private void buildNewTree(PlanNode plan,
            HashMap<EmptyTupleSourceOperator, ILogicalOperator> joinLeafInputsHashMap) {
        ILogicalOperator leftInput = joinLeafInputsHashMap.get(plan.getEmptyTupleSourceOp());
        if (leftInput.getOperatorTag() == LogicalOperatorTag.SELECT) {
            addCardCostAnnotations(leftInput, plan);
        }
    }

    private void buildNewTree(PlanNode plan, HashMap<EmptyTupleSourceOperator, ILogicalOperator> joinLeafInputsHashMap,
            List<ILogicalOperator> joinOps, MutableInt totalNumberOfJoins) {
        // we have to move the inputs in op around so that they match the tree structure in pn
        // we use the existing joinOps and switch the leafInputs appropriately.
        List<PlanNode> allPlans = joinEnum.getAllPlans();
        int leftIndex = plan.getLeftPlanIndex();
        int rightIndex = plan.getRightPlanIndex();
        PlanNode leftPlan = allPlans.get(leftIndex);
        PlanNode rightPlan = allPlans.get(rightIndex);
        ILogicalOperator joinOp = joinOps.get(totalNumberOfJoins.intValue());

        if (plan.IsJoinNode()) {
            AbstractBinaryJoinOperator abJoinOp = (AbstractBinaryJoinOperator) joinOp;
            ILogicalExpression expr = plan.getJoinExpr();
            abJoinOp.getCondition().setValue(expr);
            // add the annotations
            if (plan.getJoinOp() == PlanNode.JoinMethod.INDEX_NESTED_LOOP_JOIN) {
                // this annotation is needed for the physical optimizer to replace this with the unnest operator later
                AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) expr;
                removeJoinAnnotations(afcExpr);
                setAnnotation(afcExpr, IndexedNLJoinExpressionAnnotation.INSTANCE_ANY_INDEX);
            } else if (plan.getJoinOp() == PlanNode.JoinMethod.HYBRID_HASH_JOIN
                    || plan.getJoinOp() == PlanNode.JoinMethod.BROADCAST_HASH_JOIN
                    || plan.getJoinOp() == PlanNode.JoinMethod.CARTESIAN_PRODUCT_JOIN) {
                if (plan.getJoinOp() == PlanNode.JoinMethod.BROADCAST_HASH_JOIN) {
                    // Broadcast the right branch.
                    BroadcastExpressionAnnotation bcast =
                            new BroadcastExpressionAnnotation(plan.side == HashJoinExpressionAnnotation.BuildSide.RIGHT
                                    ? BroadcastExpressionAnnotation.BroadcastSide.RIGHT
                                    : BroadcastExpressionAnnotation.BroadcastSide.LEFT);
                    AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) expr;
                    removeJoinAnnotations(afcExpr);
                    setAnnotation(afcExpr, bcast);
                } else if (plan.getJoinOp() == PlanNode.JoinMethod.HYBRID_HASH_JOIN) {
                    HashJoinExpressionAnnotation hjAnnotation = new HashJoinExpressionAnnotation(plan.side);
                    AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) expr;
                    removeJoinAnnotations(afcExpr);
                    setAnnotation(afcExpr, hjAnnotation);
                } else {
                    if (expr != ConstantExpression.TRUE) {
                        AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) expr;
                        removeJoinAnnotations(afcExpr);
                    }
                }
            }
            addCardCostAnnotations(joinOp, plan);
        }

        if (leftPlan.IsScanNode()) {
            // leaf
            ILogicalOperator leftInput = joinLeafInputsHashMap.get(leftPlan.getEmptyTupleSourceOp());
            if (leftInput.getOperatorTag() == LogicalOperatorTag.SELECT) {
                addCardCostAnnotations(leftInput, leftPlan);
            }
            joinOp.getInputs().get(0).setValue(leftInput);
            addCardCostAnnotations(findDataSourceScanOperator(leftInput), leftPlan);
        } else {
            // join
            totalNumberOfJoins.increment();
            ILogicalOperator leftInput = joinOps.get(totalNumberOfJoins.intValue());
            joinOp.getInputs().get(0).setValue(leftInput);
            buildNewTree(allPlans.get(leftIndex), joinLeafInputsHashMap, joinOps, totalNumberOfJoins);
        }

        if (rightPlan.IsScanNode()) {
            // leaf
            ILogicalOperator rightInput = joinLeafInputsHashMap.get(rightPlan.getEmptyTupleSourceOp());
            if (rightInput.getOperatorTag() == LogicalOperatorTag.SELECT) {
                addCardCostAnnotations(rightInput, rightPlan);
            }
            joinOp.getInputs().get(1).setValue(rightInput);
            addCardCostAnnotations(findDataSourceScanOperator(rightInput), rightPlan);
        } else {
            // join
            totalNumberOfJoins.increment();
            ILogicalOperator rightInput = joinOps.get(totalNumberOfJoins.intValue());
            joinOp.getInputs().get(1).setValue(rightInput);
            buildNewTree(allPlans.get(rightIndex), joinLeafInputsHashMap, joinOps, totalNumberOfJoins);
        }
    }

    // in some very rare cases, there is an internal edge that has an assign statement such as $$var = 20 but this variable
    // is not used anywhere in the current join graph but is used outside the current join graph. So we add this assign to the top of
    // our plan, so the rest of the code will be happy. Strange that this assign appears in the join graph.
    private ILogicalOperator addConstantInternalEdgesAtTheTop(ILogicalOperator op,
            List<ILogicalOperator> internalEdges) {
        if (internalEdges.size() == 0) {
            return op;
        }
        ILogicalOperator root = op;
        for (ILogicalOperator ie : internalEdges) {
            // this will be an Assign, so no need to check
            AssignOperator aOp = (AssignOperator) ie;
            aOp.getInputs().get(0).setValue(root);
            root = aOp;
        }
        return root;
    }

    public static void printPlan(IPlanPrettyPrinter pp, AbstractLogicalOperator op, String text)
            throws AlgebricksException {
        if (LOGGER.isTraceEnabled()) {
            pp.reset();
            pp.printOperator(op, true, false);
            LOGGER.trace("---------------------------- {}\n{}\n----------------------------", text, pp);
        }
    }

    private void printLeafPlans(IPlanPrettyPrinter pp,
            HashMap<EmptyTupleSourceOperator, ILogicalOperator> joinLeafInputsHashMap) throws AlgebricksException {
        Iterator<Map.Entry<EmptyTupleSourceOperator, ILogicalOperator>> li =
                joinLeafInputsHashMap.entrySet().iterator();
        int i = 0;
        while (li.hasNext()) {
            Map.Entry<EmptyTupleSourceOperator, ILogicalOperator> pair = li.next();
            ILogicalOperator element = pair.getValue();
            printPlan(pp, (AbstractLogicalOperator) element, "Printing Leaf Input" + i);
            i++;
        }
    }

    private boolean allEqualityPreds(ILogicalExpression expr) {
        List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
        // check that the expr is AND(EQ(), EQ(),..)
        if (expr.splitIntoConjuncts(conjs)) {
            for (Mutable<ILogicalExpression> conj : conjs) {
                if (!(((AbstractFunctionCallExpression) conj.getValue()).getFunctionIdentifier()
                        .equals(AlgebricksBuiltinFunctions.EQ))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    // This routine should not be needed! When the same variable is used multiple times in a join predicate as in
    // AND(eq($$25, $$27), eq($$25, $$34)), JoinUtils.isHashJoinCondition() returns false which makes no sense.
    // Tried changing the above routine but it always lead to some failures. Unable to figure out what the problem was,
    // we just replace the duplicate occurrence of every variable with a new variable following by appropriate assign.
    // We do this only once just once before we construct the final plan.
    private void checkForMultipleUsesOfVariablesInJoinPreds(PlanNode plan,
            HashMap<EmptyTupleSourceOperator, ILogicalOperator> joinLeafInputsHashMap, IOptimizationContext context)
            throws AlgebricksException {
        List<PlanNode> allPlans = joinEnum.getAllPlans();
        if (plan.IsJoinNode()) {
            ILogicalExpression exp = plan.getJoinExpr();
            if (!allEqualityPreds(exp)) {
                return;
            }
            boolean changes = true;
            while (changes) {
                changes = false;
                List<LogicalVariable> vars = new ArrayList<>();
                exp.getUsedVariables(vars);
                Set<LogicalVariable> set = new LinkedHashSet<>(vars);
                if (set.size() < vars.size()) {
                    // walk thru vars and find the first instance of the duplicate
                    for (int i = 0; i < vars.size() - 1; i++) {
                        for (int j = i + 1; j < vars.size(); j++) {
                            if (vars.get(i) == vars.get(j)) {
                                /// find the leafInout that contains this vars(i)
                                for (Map.Entry<EmptyTupleSourceOperator, ILogicalOperator> mapElement : joinLeafInputsHashMap
                                        .entrySet()) {
                                    ILogicalOperator joinLeafInput = mapElement.getValue();
                                    EmptyTupleSourceOperator ets = mapElement.getKey();
                                    HashSet<LogicalVariable> vars2 = new HashSet<>();
                                    VariableUtilities.getLiveVariables(joinLeafInput, vars2);
                                    if (vars2.contains(vars.get(i))) {
                                        LogicalVariable newVar = context.newVar();
                                        // replace one occurrence of vars(i) in exp
                                        substituteVarOnce(exp, vars.get(i), newVar);
                                        VariableReferenceExpression oldvarExpr =
                                                new VariableReferenceExpression(vars.get(i));
                                        AssignOperator assign =
                                                new AssignOperator(newVar, new MutableObject<>(oldvarExpr));
                                        // Now add an assign to the joinLeafInput : newvar <-- oldvar
                                        assign.getInputs().add(new MutableObject<>(joinLeafInput));
                                        context.computeAndSetTypeEnvironmentForOperator(assign);
                                        context.addNotToBeInlinedVar(newVar);
                                        context.addNotToBeInlinedVar(vars.get(i));

                                        // also update the joinLeafInputsHashMap
                                        joinLeafInputsHashMap.put(ets, assign);
                                        changes = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // now traverse left and right side plans
            int leftIndex = plan.getLeftPlanIndex();
            int rightIndex = plan.getRightPlanIndex();
            PlanNode leftPlan = allPlans.get(leftIndex);
            PlanNode rightPlan = allPlans.get(rightIndex);
            if (leftPlan.IsJoinNode()) {
                checkForMultipleUsesOfVariablesInJoinPreds(leftPlan, joinLeafInputsHashMap, context);
            }
            if (rightPlan.IsJoinNode()) {
                checkForMultipleUsesOfVariablesInJoinPreds(rightPlan, joinLeafInputsHashMap, context);
            }
        }
    }

    // for every internal edge assign (again assuming only 1 for now), find the corresponding leafInput and place the assign
    // on top of that LeafInput. Modify the joinLeafInputsHashMap as well.
    private void pushAssignsIntoLeafInputs(HashMap<EmptyTupleSourceOperator, ILogicalOperator> joinLeafInputsHashMap,
            List<ILogicalOperator> internalEdges) throws AlgebricksException {

        for (Map.Entry<EmptyTupleSourceOperator, ILogicalOperator> mapElement : joinLeafInputsHashMap.entrySet()) {
            ILogicalOperator joinLeafInput = mapElement.getValue();
            EmptyTupleSourceOperator ets = mapElement.getKey();
            int internalEdgeNumber = findInternalEdge(joinLeafInput, internalEdges);
            if (internalEdgeNumber != -1) {
                joinLeafInput = addAssignToLeafInput(joinLeafInput, internalEdges.get(internalEdgeNumber));
                joinLeafInputsHashMap.put(ets, joinLeafInput);
                internalEdges.remove(internalEdgeNumber); // no longer needed
            }
        }

    }

    private boolean substituteVarOnce(ILogicalExpression exp, LogicalVariable oldVar, LogicalVariable newVar) {
        switch (exp.getExpressionTag()) {
            case FUNCTION_CALL:
                AbstractFunctionCallExpression fun = (AbstractFunctionCallExpression) exp;
                for (int i = 0; i < fun.getArguments().size(); i++) {
                    ILogicalExpression arg = fun.getArguments().get(i).getValue();
                    if (substituteVarOnce(arg, oldVar, newVar)) {
                        return true;
                    }
                }
                return false;
            case VARIABLE:
                VariableReferenceExpression varExpr = (VariableReferenceExpression) exp;
                if (varExpr.getVariableReference().equals(oldVar)) {
                    varExpr.setVariable(newVar);
                    return true;
                }
                return false;
            default:
                return false;
        }
    }

    // check to see if every dataset has a sample. If not, CBO code cannot run. A warning message must be issued as well.
    private boolean doAllDataSourcesHaveSamples(
            List<Pair<EmptyTupleSourceOperator, DataSourceScanOperator>> emptyTupleAndDataSourceOps,
            IOptimizationContext context) throws AlgebricksException {
        for (Pair<EmptyTupleSourceOperator, DataSourceScanOperator> emptyTupleAndDataSourceOp : emptyTupleAndDataSourceOps) {
            if (emptyTupleAndDataSourceOp.getSecond() != null) {
                DataSourceScanOperator scanOp = emptyTupleAndDataSourceOp.getSecond();
                Index index = joinEnum.getStatsHandle().findSampleIndex(scanOp, context);
                if (index == null) {
                    return false;
                }
            }
        }
        return true;
    }
}
