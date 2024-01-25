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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.annotations.IndexedNLJoinExpressionAnnotation;
import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.metadata.entities.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.HashJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EnumerateJoinsRule implements IAlgebraicRewriteRule {

    private static final Logger LOGGER = LogManager.getLogger();

    private final JoinEnum joinEnum;
    private int leafInputNumber;
    private List<ILogicalOperator> newJoinOps;
    private List<JoinOperator> allJoinOps; // can be inner join or left outer join
    // Will be in the order of the from clause. Important for position ordering when assigning bits to join expressions.
    private List<ILogicalOperator> leafInputs;
    private HashMap<LogicalVariable, Integer> varLeafInputIds;
    private List<Triple<Integer, Integer, Boolean>> buildSets; // the first is the bits and the second is the number of tables.
    private List<Quadruple<Integer, Integer, JoinOperator, Integer>> outerJoinsDependencyList;
    private List<AssignOperator> assignOps;
    private List<ILogicalExpression> assignJoinExprs; // These are the join expressions below the assign operator.

    // The Distinct operators for each DataSourceScan operator (if applicable)
    private HashMap<DataSourceScanOperator, ILogicalOperator> dataScanAndGroupByDistinctOps;

    // The Distinct/GroupBy operator at root of the query tree (if exists)
    private ILogicalOperator rootGroupByDistinctOp;

    // The OrderBy operator at root of the query tree (if exists)
    private ILogicalOperator rootOrderByOp;

    private List<LogicalVariable> resultAndJoinVars = new ArrayList();

    public EnumerateJoinsRule(JoinEnum joinEnum) {
        this.joinEnum = joinEnum;
        dataScanAndGroupByDistinctOps = new HashMap<>(); // initialized only once at the beginning of the rule
        rootGroupByDistinctOp = null;
        rootOrderByOp = null;
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
        if (!(joinClause(op) || ((op.getOperatorTag() == LogicalOperatorTag.DISTRIBUTE_RESULT)))) {
            return false;
        }

        if (op.getOperatorTag() == LogicalOperatorTag.DISTRIBUTE_RESULT) {
            // If cboMode or cboTestMode is true, identify each DistinctOp or GroupByOp for the corresponding DataScanOp
            getDistinctOpsForJoinNodes(op, context);

            // Find the order by op, so we can annotate cost/cards
            findOrderByOp(op);

            // Find the topmost assign, so we can find all the final projected variables.
            ILogicalOperator tmp = op;

            while (tmp.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
                if (tmp.getOperatorTag().equals(LogicalOperatorTag.ASSIGN)) {
                    addAllAssignExprVars(resultAndJoinVars, (AssignOperator) tmp);
                    break;
                }
                tmp = tmp.getInputs().get(0).getValue();
            }
        }

        // if this join has already been seen before, no need to apply the rule again
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        //joinOps = new ArrayList<>();
        allJoinOps = new ArrayList<>();
        newJoinOps = new ArrayList<>();
        leafInputs = new ArrayList<>();
        varLeafInputIds = new HashMap<>();
        outerJoinsDependencyList = new ArrayList<>();
        assignOps = new ArrayList<>();
        assignJoinExprs = new ArrayList<>();
        buildSets = new ArrayList<>();
        IPlanPrettyPrinter pp = context.getPrettyPrinter();
        printPlan(pp, (AbstractLogicalOperator) op, "Original Whole plan1");
        leafInputNumber = 0;
        boolean canTransform = getJoinOpsAndLeafInputs(op);

        if (!canTransform) {
            return false;
        }

        collectJoinConditionsVariables(); // will be used for determining which variables will be projected from the base levels

        convertOuterJoinstoJoinsIfPossible(outerJoinsDependencyList);

        printPlan(pp, (AbstractLogicalOperator) op, "Original Whole plan2");
        int numberOfFromTerms = leafInputs.size();

        if (LOGGER.isTraceEnabled()) {
            String viewInPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
            LOGGER.trace("viewInPlan");
            LOGGER.trace(viewInPlan);
        }

        if (buildSets.size() > 1) {
            buildSets.sort(Comparator.comparingDouble(o -> o.second)); // sort on the number of tables in each set
            // we need to build the smaller sets first. So we need to find these first.
        }
        joinEnum.initEnum((AbstractLogicalOperator) op, cboMode, cboTestMode, numberOfFromTerms, leafInputs, allJoinOps,
                assignOps, outerJoinsDependencyList, buildSets, varLeafInputIds, dataScanAndGroupByDistinctOps,
                rootGroupByDistinctOp, rootOrderByOp, resultAndJoinVars, context);

        if (cboMode) {
            if (!doAllDataSourcesHaveSamples(leafInputs, context)) {
                return false;
            }
        }

        printLeafPlans(pp, leafInputs, "Inputs1");

        if (assignOps.size() > 0) {
            pushAssignsIntoLeafInputs(pp, leafInputs, assignOps, assignJoinExprs);
        }

        printLeafPlans(pp, leafInputs, "Inputs2");

        int cheapestPlan = joinEnum.enumerateJoins(); // MAIN CALL INTO CBO
        if (cheapestPlan == PlanNode.NO_PLAN) {
            return false;
        }

        PlanNode cheapestPlanNode = joinEnum.allPlans.get(cheapestPlan);

        generateHintWarnings();

        if (numberOfFromTerms > 1) {
            getNewJoinOps(cheapestPlanNode, allJoinOps);
            if (allJoinOps.size() != newJoinOps.size()) {
                return false; // there are some cases such as R OJ S on true. Here there is an OJ predicate but the code in findJoinConditions
                // in JoinEnum does not capture this. Will fix later. Just bail for now.
            }
            buildNewTree(cheapestPlanNode, newJoinOps, new MutableInt(0), context);
            opRef.setValue(newJoinOps.get(0));

            if (assignOps.size() > 0) {
                for (int i = assignOps.size() - 1; i >= 0; i--) {
                    MutableBoolean removed = new MutableBoolean(false);
                    removed.setFalse();
                    pushAssignsAboveJoins(newJoinOps.get(0), assignOps.get(i), assignJoinExprs.get(i), removed);
                    context.computeAndSetTypeEnvironmentForOperator(newJoinOps.get(i));
                    context.computeAndSetTypeEnvironmentForOperator(assignOps.get(i));
                    if (removed.isTrue()) {
                        assignOps.remove(i);
                    }
                }
            }

            printPlan(pp, (AbstractLogicalOperator) newJoinOps.get(0), "New Whole Plan after buildNewTree 1");
            ILogicalOperator root = addRemainingAssignsAtTheTop(newJoinOps.get(0), assignOps);
            printPlan(pp, (AbstractLogicalOperator) newJoinOps.get(0), "New Whole Plan after buildNewTree 2");
            printPlan(pp, (AbstractLogicalOperator) root, "New Whole Plan after buildNewTree");

            // this will be the new root
            opRef.setValue(root);

            if (LOGGER.isTraceEnabled()) {
                String viewOutPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
                LOGGER.trace("viewOutPlan");
                LOGGER.trace(viewOutPlan);
            }

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("---------------------------- Printing Leaf Inputs");
                printLeafPlans(pp, leafInputs, "Inputs");
                // print joins starting from the bottom
                for (int i = newJoinOps.size() - 1; i >= 0; i--) {
                    printPlan(pp, (AbstractLogicalOperator) newJoinOps.get(i), "join " + i);
                }
                printPlan(pp, (AbstractLogicalOperator) newJoinOps.get(0), "New Whole Plan");
                printPlan(pp, (AbstractLogicalOperator) root, "New Whole Plan");
            }
            // turn of this rule for all joins in this set (subtree)
            for (ILogicalOperator joinOp : newJoinOps) {
                context.addToDontApplySet(this, joinOp);
            }
        } else {
            buildNewTree(cheapestPlanNode);
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
        return true;
    }

    private void collectJoinConditionsVariables() {
        for (JoinOperator jOp : allJoinOps) {
            AbstractBinaryJoinOperator joinOp = jOp.getAbstractJoinOp();
            ILogicalExpression expr = joinOp.getCondition().getValue();
            List<LogicalVariable> vars = new ArrayList<>();
            expr.getUsedVariables(vars);
            resultAndJoinVars.addAll(vars); // collect all the variables used in join expressions. These will be projected from the base level
        }
    }

    private void addAllAssignExprVars(List<LogicalVariable> resultAndJoinVars, AssignOperator op) {
        for (Mutable<ILogicalExpression> exp : op.getExpressions()) {
            List<LogicalVariable> vars = new ArrayList<>();
            exp.getValue().getUsedVariables(vars);
            resultAndJoinVars.addAll(vars);
        }
    }

    private void pushAssignsAboveJoins(ILogicalOperator op, AssignOperator aOp, ILogicalExpression jexpr,
            MutableBoolean removed) {
        System.out.println("op " + op.toString());
        if (!op.getInputs().isEmpty()) {
            for (int i = 0; i < op.getInputs().size(); i++) {
                ILogicalOperator oper = op.getInputs().get(i).getValue();
                if (joinClause(oper)) {
                    AbstractBinaryJoinOperator abOp = (AbstractBinaryJoinOperator) oper;
                    ILogicalExpression expr = abOp.getCondition().getValue();
                    if (expr.equals(jexpr)) {
                        op.getInputs().get(i).setValue(aOp);
                        aOp.getInputs().get(0).setValue(oper);
                        removed.setTrue();
                        return;
                    }
                }
                pushAssignsAboveJoins(oper, aOp, jexpr, removed);
            }
        }
    }

    private boolean joinClause(ILogicalOperator op) {
        if (op.getOperatorTag() == LogicalOperatorTag.INNERJOIN)
            return true;
        if (op.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN)
            return true;
        return false;
    }

    private void generateHintWarnings() {
        for (Map.Entry<IExpressionAnnotation, Warning> mapElement : joinEnum.joinHints.entrySet()) {
            IExpressionAnnotation annotation = mapElement.getKey();
            Warning warning = mapElement.getValue();
            if (warning != null) {
                IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(warning);
                }
            }
        }
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
    private boolean onlyOneAssign(ILogicalOperator op, List<AssignOperator> assignOps) {
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator aOp = (AssignOperator) op;
            assignOps.add(aOp);
            op = op.getInputs().get(0).getValue();
        }
        return (joinClause(op));
    }

    // An internal edge must contain only assigns followed by an inner join. Not sure if there will be other ops between joins
    private int numVarRefExprs(AssignOperator aOp) {
        List<Mutable<ILogicalExpression>> exprs = aOp.getExpressions();
        int count = 0;
        for (Mutable<ILogicalExpression> exp : exprs) {
            if (exp.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) exp.getValue();
                for (Mutable<ILogicalExpression> arg : afcExpr.getArguments()) {
                    if (arg.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                        count++;
                    }
                }
            }
        }

        return count;
    }

    private boolean onlyAssigns(ILogicalOperator op, List<AssignOperator> assignOps) {
        while (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator aOp = (AssignOperator) op;
            int count = numVarRefExprs(aOp);
            if (count > 1) {
                return false;
            }
            assignOps.add(aOp);
            assignJoinExprs.add(joinExprFound(op));
            op = op.getInputs().get(0).getValue();
        }
        return (joinClause(op));
    }

    private ILogicalOperator skipPastAssigns(ILogicalOperator nextOp) {
        while (nextOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            nextOp = nextOp.getInputs().get(0).getValue();
        }
        return nextOp;
    }

    private ILogicalOperator findSelectOrDataScan(ILogicalOperator op) {
        LogicalOperatorTag tag;
        ILogicalOperator currentOp = op;
        while (true) {
            if (currentOp.getInputs().size() > 1) {
                return null; // Assuming only a linear plan for single table queries (as leafInputs are linear).
            }
            tag = currentOp.getOperatorTag();
            if (tag == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                return null; // if this happens, there is nothing we can do in CBO code since there is no datasourcescan
            }
            if ((tag == LogicalOperatorTag.SELECT) || (tag == LogicalOperatorTag.DATASOURCESCAN)) {
                return currentOp;
            }

            currentOp = currentOp.getInputs().get(0).getValue();
        }
    }

    private void getDistinctOpsForJoinNodes(ILogicalOperator op, IOptimizationContext context) {
        if (op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            return;
        }
        ILogicalOperator grpByDistinctOp = null; // null indicates no DistinctOp or GroupByOp
        DataSourceScanOperator scanOp;
        ILogicalOperator currentOp = op;
        while (true) {
            LogicalOperatorTag tag = currentOp.getOperatorTag();
            if (tag == LogicalOperatorTag.DISTINCT || tag == LogicalOperatorTag.GROUP) {
                grpByDistinctOp = currentOp; // GroupByOp Variable expressions (if any) take over DistinctOp ones
                this.rootGroupByDistinctOp = grpByDistinctOp;
            } else if (tag == LogicalOperatorTag.INNERJOIN || tag == LogicalOperatorTag.LEFTOUTERJOIN) {
                if (grpByDistinctOp != null) {
                    Pair<List<LogicalVariable>, List<AbstractFunctionCallExpression>> distinctVarsFuncPair =
                            OperatorUtils.getGroupByDistinctVarFuncPair(grpByDistinctOp);
                    for (int i = 0; i < currentOp.getInputs().size(); i++) {
                        ILogicalOperator nextOp = currentOp.getInputs().get(i).getValue();
                        OperatorUtils.createDistinctOpsForJoinNodes(nextOp, distinctVarsFuncPair, context,
                                dataScanAndGroupByDistinctOps);
                    }
                }
                return;
            } else if (tag == LogicalOperatorTag.DATASOURCESCAN) { // single table queries
                scanOp = (DataSourceScanOperator) currentOp;
                // will work for any attributes present in GroupByOp or DistinctOp
                if (grpByDistinctOp != null) {
                    dataScanAndGroupByDistinctOps.put(scanOp, grpByDistinctOp);
                }
                return;
            }
            currentOp = currentOp.getInputs().get(0).getValue();
            if (currentOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                return; // if this happens, there is nothing we can do in CBO code since there is no DataSourceScan
            }
        }
    }

    private void findOrderByOp(ILogicalOperator op) {
        ILogicalOperator currentOp = op;
        if (currentOp.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            return;
        }

        while (currentOp != null) {
            LogicalOperatorTag tag = currentOp.getOperatorTag();
            if (tag == LogicalOperatorTag.ORDER) {
                this.rootOrderByOp = currentOp;
                return;
            }
            if (tag == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                return;
            }
            currentOp = currentOp.getInputs().get(0).getValue();
        }
    }

    private int getLeafInputId(LogicalVariable lv) {
        if (varLeafInputIds.containsKey(lv))
            return varLeafInputIds.get(lv);
        return -1;
    }

    private boolean addLeafInputNumbersToVars(ILogicalOperator op) throws AlgebricksException {
        HashSet<LogicalVariable> opVars = new HashSet<>();
        VariableUtilities.getLiveVariables(op, opVars);
        for (LogicalVariable lv : opVars) {
            int id = getLeafInputId(lv);
            if ((id != -1) && (id != leafInputNumber)) {
                return false; // this should not happen
                // the same variable in different leaf Inputs is problematic for CBO
            }
            varLeafInputIds.put(lv, leafInputNumber);
        }
        return true;
    }

    private boolean foundVar(LogicalVariable inputLV, ILogicalOperator op) throws AlgebricksException {
        HashSet<LogicalVariable> opVars = new HashSet<>();
        VariableUtilities.getLiveVariables(op, opVars);
        if (opVars.contains(inputLV)) { // note that this will fail if there variables from different leafInputs
            return true;
        }
        return false;
    }

    // dependencylist is  first, second, op
    // If we have R outer join S, first is the null extending table as in R, null
    // In this case, if S is to joined, then R must be present. So S depends on R.
    // If we have a case of (first, second, LOJ_operator) = (R_leaf_input_id, S_leaf_input_id, LOJop),
    // and another (S_leaf_input_id, ..., joinOp),
    // OR (..., S_leaf_input_id, joinOp) then the LOJ can be converted to a join!!
    private void convertOuterJoinstoJoinsIfPossible(
            List<Quadruple<Integer, Integer, JoinOperator, Integer>> outerJoinsDependencyList) {
        int i, j;
        boolean changes = true;
        while (changes) {
            changes = false;
            for (i = 0; i < outerJoinsDependencyList.size(); i++) {
                Quadruple<Integer, Integer, JoinOperator, Integer> tr1 = outerJoinsDependencyList.get(i);
                if (tr1.getThird().getOuterJoin()) {
                    for (j = 0; j < outerJoinsDependencyList.size(); j++) {
                        Quadruple<Integer, Integer, JoinOperator, Integer> tr2 = outerJoinsDependencyList.get(j);
                        if ((i != j) && !(tr2.getThird().getOuterJoin())) {
                            if ((tr1.getSecond().equals(tr2.getFirst())) || (tr1.getSecond().equals(tr2.getSecond()))) {
                                outerJoinsDependencyList.get(i).getThird().setOuterJoin(false);
                                changes = true;
                            }
                        }
                    }
                }
            }
        }

        // now remove all joins from the list, as we do not need them anymore! We only need the outer joins
        for (i = outerJoinsDependencyList.size() - 1; i >= 0; i--) {
            if (!outerJoinsDependencyList.get(i).getThird().getOuterJoin()) { // not an outerjoin
                outerJoinsDependencyList.remove(i);
            }
        }

        if (outerJoinsDependencyList.size() == 0) {
            for (i = buildSets.size() - 1; i >= 0; i--) {
                buildSets.remove(i); // no need for buildSets if there are no OJs.
            }
        }
    }

    // Each outer join will create one set of dependencies. The right side depends on the left side.
    private boolean buildDependencyList(ILogicalOperator op, JoinOperator jO,
            List<Quadruple<Integer, Integer, JoinOperator, Integer>> outerJoinsDependencyList, int rightSideBits)
            throws AlgebricksException {
        AbstractBinaryJoinOperator outerJoinOp = (AbstractBinaryJoinOperator) op;
        ILogicalOperator leftOp = op.getInputs().get(0).getValue();
        ILogicalExpression expr = outerJoinOp.getCondition().getValue();
        int leftSideExprBits, rightSideExprBits;
        List<LogicalVariable> joinExprVars;
        List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
        if (expr.splitIntoConjuncts(conjs)) {
            for (Mutable<ILogicalExpression> conj : conjs) {
                joinExprVars = new ArrayList<>();
                leftSideExprBits = 0;
                rightSideExprBits = 0;
                conj.getValue().getUsedVariables(joinExprVars);
                for (LogicalVariable lv : joinExprVars) {
                    int id = getLeafInputId(lv);
                    if (id != -1) {
                        if (foundVar(lv, leftOp)) {
                            leftSideExprBits |= 1 << (id - 1);
                        } else {
                            rightSideExprBits |= 1 << (id - 1);
                        }
                    }
                }
                if (leftSideExprBits != 0 && rightSideExprBits != 0) {// avoid expressions like true
                    outerJoinsDependencyList.add(new Quadruple(leftSideExprBits, rightSideBits, jO, 0));
                }
            }
        } else {
            leftSideExprBits = 0;
            rightSideExprBits = 0;
            joinExprVars = new ArrayList<>();
            expr.getUsedVariables(joinExprVars);
            for (LogicalVariable lv : joinExprVars) {
                int id = getLeafInputId(lv);
                if (id != -1) {
                    if (foundVar(lv, leftOp)) {
                        leftSideExprBits |= 1 << (id - 1);
                    } else {
                        rightSideExprBits |= 1 << (id - 1);
                    }
                }
            }
            if (leftSideExprBits != 0 && rightSideExprBits != 0) {// avoid expressions like true
                outerJoinsDependencyList.add(new Quadruple(leftSideExprBits, rightSideBits, jO, 0));
            }
        }
        return true;
    }

    private ILogicalExpression joinExprFound(ILogicalOperator op) {
        if (!op.getInputs().isEmpty()) {
            for (int i = 0; i < op.getInputs().size(); i++) {
                ILogicalOperator oper = op.getInputs().get(i).getValue();
                if (joinClause(oper)) {
                    AbstractBinaryJoinOperator abOp = (AbstractBinaryJoinOperator) oper;
                    return abOp.getCondition().getValue();
                }
                return joinExprFound(oper);
            }
        } else {
            return null;
        }
        return null;
    }

    /**
     * This is the main routine that stores all the join operators and the leafInputs. We will later reuse the same
     * join operators but switch the leafInputs (see buildNewTree). The whole scheme is based on the assumption that the
     * leafInputs can be switched. The various data structures make the leafInputs accessible efficiently.
     */
    private boolean getJoinOpsAndLeafInputs(ILogicalOperator op) throws AlgebricksException {
        if (joinClause(op)) {
            JoinOperator jO = new JoinOperator((AbstractBinaryJoinOperator) op);
            allJoinOps.add(jO);
            if (op.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
                jO.setOuterJoin(true);
            }

            int firstLeafInputNumber, lastLeafInputNumber;
            int k = 0;
            for (int i = 0; i < 2; i++) {
                ILogicalOperator nextOp = op.getInputs().get(i).getValue();
                firstLeafInputNumber = leafInputNumber + 1; // we are interested in the 2nd input only
                boolean canTransform = getJoinOpsAndLeafInputs(nextOp);
                if (!canTransform) {
                    return false;
                }
                lastLeafInputNumber = leafInputNumber; // we are interested in the 2nd input only
                k = 0;
                // now we know the leafInput numbers that occurred on the right side of this join.
                //if ((op.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) && (i == 1)) {
                if ((joinClause(op)) && (i == 1)) {
                    for (int j = firstLeafInputNumber; j <= lastLeafInputNumber; j++) {
                        k |= 1 << (j - 1);
                    }
                    // buildSets are only for outerjoins.
                    if ((op.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN)
                            && (firstLeafInputNumber < lastLeafInputNumber)) { // if more is than one leafInput, only then buildSets make sense.
                        buildSets.add(new Triple<>(k, lastLeafInputNumber - firstLeafInputNumber + 1, true)); // convert the second to boolean later
                    }
                    boolean ret = buildDependencyList(op, jO, outerJoinsDependencyList, k);
                    if (!ret) {
                        return false;
                    }
                }
            }
        } else {
            if (op.getOperatorTag() == LogicalOperatorTag.GROUP) { // cannot handle group by's in leaf Inputs.
                return false;
            }
            Pair<EmptyTupleSourceOperator, DataSourceScanOperator> etsDataSource = containsLeafInputOnly(op);
            if (etsDataSource != null) { // a leaf input
                EmptyTupleSourceOperator etsOp = etsDataSource.first;
                DataSourceScanOperator dataSourceOp = etsDataSource.second;
                if (op.getOperatorTag().equals(LogicalOperatorTag.DISTRIBUTE_RESULT)) {// single table query
                    ILogicalOperator selectOp = findSelectOrDataScan(op);
                    if (selectOp == null) {
                        return false;
                    } else {
                        leafInputs.add(selectOp);
                    }
                } else {
                    leafInputNumber++;
                    leafInputs.add(op);
                    if (!addLeafInputNumbersToVars(op)) {
                        return false;
                    }
                }
            } else { // This must be an internal edge
                if (onlyAssigns(op, assignOps)) {
                    ILogicalOperator skipAssisgnsOp = skipPastAssigns(op);
                    boolean canTransform = getJoinOpsAndLeafInputs(skipAssisgnsOp);
                    if (!canTransform) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    private void addCardCostAnnotations(ILogicalOperator op, PlanNode plan) {
        if (op == null)
            return;
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
        return null;
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

    private int findAssignOp(ILogicalOperator leafInput, List<AssignOperator> assignOps,
            List<ILogicalExpression> assignJoinExprs) throws AlgebricksException {
        int i = -1;
        for (AssignOperator aOp : assignOps) {
            i++;
            if (assignJoinExprs.get(i) != null)
                continue; // this is an assign associated with a join expression
            // this will be an Assign, so no need to check
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

    private ILogicalOperator addAssignToLeafInput(ILogicalOperator leafInput, AssignOperator aOp) {
        // this will be an Assign, so no need to check
        aOp.getInputs().get(0).setValue(leafInput);
        return aOp;
    }

    private void skipAllIndexes(PlanNode plan, ILogicalOperator leafInput) {
        if (plan.scanOp == PlanNode.ScanMethod.TABLE_SCAN && leafInput.getOperatorTag() == LogicalOperatorTag.SELECT) {
            SelectOperator selOper = (SelectOperator) leafInput;
            ILogicalExpression expr = selOper.getCondition().getValue();
            List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
            if (expr.splitIntoConjuncts(conjs)) {
                conjs.remove(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
                for (Mutable<ILogicalExpression> conj : conjs) {
                    if (conj.getValue().getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                        AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) conj.getValue();
                        // remove any annotations that may have been here from other parts of the code. We know we want a datascan.
                        afce.removeAnnotation(SkipSecondaryIndexSearchExpressionAnnotation.class);
                        afce.putAnnotation(SkipSecondaryIndexSearchExpressionAnnotation.INSTANCE_ANY_INDEX);
                    }
                }
            } else {
                if ((expr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL))) {
                    AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) expr;
                    // remove any annotations that may have been here from other parts of the code. We know we want a datascan.
                    afce.removeAnnotation(SkipSecondaryIndexSearchExpressionAnnotation.class);
                    afce.putAnnotation(SkipSecondaryIndexSearchExpressionAnnotation.INSTANCE_ANY_INDEX);
                }
            }
        }
    }

    // This is for single table queries
    private void buildNewTree(PlanNode plan) {
        ILogicalOperator leftInput = plan.getLeafInput();
        skipAllIndexes(plan, leftInput);
        ILogicalOperator selOp = findSelectOrDataScan(leftInput);
        if (selOp != null) {
            addCardCostAnnotations(selOp, plan);
        }
        addCardCostAnnotations(findDataSourceScanOperator(leftInput), plan);
    }

    private void getJoinNode(PlanNode plan, List<JoinOperator> allJoinOps) throws AlgebricksException {
        AbstractBinaryJoinOperator abjOp;
        int i;

        if (plan.outerJoin) {
            for (i = 0; i < allJoinOps.size(); i++) {
                abjOp = allJoinOps.get(i).getAbstractJoinOp();
                if (abjOp.getJoinKind() == AbstractBinaryJoinOperator.JoinKind.LEFT_OUTER) {
                    newJoinOps.add(OperatorManipulationUtil.bottomUpCopyOperators(abjOp));
                    return;
                }
            }
        } else {
            for (i = 0; i < allJoinOps.size(); i++) {
                abjOp = allJoinOps.get(i).getAbstractJoinOp();
                if (abjOp.getJoinKind() == AbstractBinaryJoinOperator.JoinKind.INNER) {
                    newJoinOps.add(OperatorManipulationUtil.bottomUpCopyOperators(abjOp));
                    return;
                }
            }
        }
    }

    private void getNewJoinOps(PlanNode plan, List<JoinOperator> allJoinOps) throws AlgebricksException {
        if (plan.IsJoinNode()) {
            getJoinNode(plan, allJoinOps);
            getNewJoinOps(plan.getLeftPlanNode(), allJoinOps);
            getNewJoinOps(plan.getRightPlanNode(), allJoinOps);
        }
    }

    private void fillJoinAnnotations(PlanNode plan, ILogicalOperator joinOp) {
        AbstractBinaryJoinOperator abJoinOp = (AbstractBinaryJoinOperator) joinOp;
        ILogicalExpression expr = plan.getJoinExpr();
        abJoinOp.getCondition().setValue(expr);
        // add the annotations
        if (plan.getJoinOp() == PlanNode.JoinMethod.INDEX_NESTED_LOOP_JOIN) {
            // this annotation is needed for the physical optimizer to replace this with the unnest operator later
            AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) expr;
            removeJoinAnnotations(afcExpr);
            setAnnotation(afcExpr, IndexedNLJoinExpressionAnnotation.INSTANCE_ANY_INDEX);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Added IndexedNLJoinExpressionAnnotation.INSTANCE_ANY_INDEX to " + afcExpr.toString());
            }
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
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Added BroadCastAnnotation to " + afcExpr.toString());
                }
            } else if (plan.getJoinOp() == PlanNode.JoinMethod.HYBRID_HASH_JOIN) {
                HashJoinExpressionAnnotation hjAnnotation = new HashJoinExpressionAnnotation(plan.side);
                AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) expr;
                removeJoinAnnotations(afcExpr);
                setAnnotation(afcExpr, hjAnnotation);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Added HashJoinAnnotation to " + afcExpr.toString());
                }
            } else {
                if (expr != ConstantExpression.TRUE) {
                    AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) expr;
                    removeJoinAnnotations(afcExpr);
                }
            }
        }
        addCardCostAnnotations(joinOp, plan);
    }

    // This one is for join queries
    private void buildNewTree(PlanNode plan, List<ILogicalOperator> joinOps, MutableInt totalNumberOfJoins,
            IOptimizationContext context) throws AlgebricksException {
        // we have to move the inputs in op around so that they match the tree structure in pn
        // we use the existing joinOps and switch the leafInputs appropriately.
        List<PlanNode> allPlans = joinEnum.getAllPlans();
        int leftIndex = plan.getLeftPlanIndex();
        int rightIndex = plan.getRightPlanIndex();
        //System.out.println("allPlansSize " + allPlans.size() + " leftIndex " + leftIndex + " rightIndex " + rightIndex); // put in trace statements
        //System.out.println("allPlansSize " + allPlans.size());
        PlanNode leftPlan = allPlans.get(leftIndex);
        PlanNode rightPlan = allPlans.get(rightIndex);

        ILogicalOperator joinOp = joinOps.get(totalNumberOfJoins.intValue()); // intValue set to 0 initially

        if (plan.IsJoinNode()) {
            fillJoinAnnotations(plan, joinOp);
        }

        if (leftPlan.IsScanNode()) {
            // leaf
            ILogicalOperator leftInput = leftPlan.getLeafInput();
            skipAllIndexes(leftPlan, leftInput);
            ILogicalOperator selOp = findSelectOrDataScan(leftInput);
            if (selOp != null) {
                addCardCostAnnotations(selOp, leftPlan);
            }
            joinOp.getInputs().get(0).setValue(leftInput);
            context.computeAndSetTypeEnvironmentForOperator(joinOp.getInputs().get(0).getValue());
            addCardCostAnnotations(findDataSourceScanOperator(leftInput), leftPlan);
        } else {
            // join
            totalNumberOfJoins.increment();
            ILogicalOperator leftInput = joinOps.get(totalNumberOfJoins.intValue());
            joinOp.getInputs().get(0).setValue(leftInput);
            context.computeAndSetTypeEnvironmentForOperator(joinOp.getInputs().get(0).getValue());
            buildNewTree(leftPlan, joinOps, totalNumberOfJoins, context);
        }

        if (rightPlan.IsScanNode()) {
            // leaf
            ILogicalOperator rightInput = rightPlan.getLeafInput();
            skipAllIndexes(rightPlan, rightInput);
            ILogicalOperator selOp = findSelectOrDataScan(rightInput);
            if (selOp != null) {
                addCardCostAnnotations(selOp, rightPlan);
            }
            joinOp.getInputs().get(1).setValue(rightInput);
            context.computeAndSetTypeEnvironmentForOperator(joinOp.getInputs().get(1).getValue());
            addCardCostAnnotations(findDataSourceScanOperator(rightInput), rightPlan);
        } else {
            // join
            totalNumberOfJoins.increment();
            ILogicalOperator rightInput = joinOps.get(totalNumberOfJoins.intValue());
            joinOp.getInputs().get(1).setValue(rightInput);
            context.computeAndSetTypeEnvironmentForOperator(joinOp.getInputs().get(1).getValue());
            buildNewTree(rightPlan, joinOps, totalNumberOfJoins, context);
        }
    }

    // in some very rare cases, there is an internal edge that has an assign statement such as $$var = 20 but this variable
    // is not used anywhere in the current join graph but is used outside the current join graph. So we add this assign to the top of
    // our plan, so the rest of the code will be happy. Strange that this assign appears in the join graph.

    private ILogicalOperator addRemainingAssignsAtTheTop(ILogicalOperator op, List<AssignOperator> assignOps) {
        ILogicalOperator root = op;
        for (AssignOperator aOp : assignOps) {
            aOp.getInputs().get(0).setValue(root);
            root = aOp;
        }
        return root;
    }

    protected static void printPlan(IPlanPrettyPrinter pp, AbstractLogicalOperator op, String text)
            throws AlgebricksException {
        if (LOGGER.isTraceEnabled()) {
            pp.reset();
            pp.printOperator(op, true, false);
            LOGGER.trace("---------------------------- {}\n{}\n----------------------------", text, pp);
        }
    }

    private void printLeafPlans(IPlanPrettyPrinter pp, List<ILogicalOperator> leafInputs, String msg)
            throws AlgebricksException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(msg);
            int i = 0;
            for (ILogicalOperator element : leafInputs) {
                printPlan(pp, (AbstractLogicalOperator) element, "Printing Leaf Input" + i);
                i++;
            }
        }
    }

    // for every internal edge assign (again assuming only 1 for now), find the corresponding leafInput and place the assign
    // on top of that LeafInput. Modify the joinLeafInputsHashMap as well.
    private void pushAssignsIntoLeafInputs(IPlanPrettyPrinter pp, List<ILogicalOperator> leafInputs,
            List<AssignOperator> assignOps, List<ILogicalExpression> assignJoinExprs) throws AlgebricksException {
        for (ILogicalOperator lo : leafInputs) {
            ILogicalOperator joinLeafInput = lo;
            printPlan(pp, (AbstractLogicalOperator) joinLeafInput, "Incoming leaf Input");
            int assignNumber = findAssignOp(joinLeafInput, assignOps, assignJoinExprs);
            if (assignNumber != -1) {
                joinLeafInput = addAssignToLeafInput(joinLeafInput, assignOps.get(assignNumber));
                printPlan(pp, (AbstractLogicalOperator) joinLeafInput, "Modified leaf Input");
                assignOps.remove(assignNumber);
            }
        }
    }

    // check to see if every dataset has a sample. If not, CBO code cannot run. A warning message must be issued as well.
    private boolean doAllDataSourcesHaveSamples(List<ILogicalOperator> leafInputs, IOptimizationContext context)
            throws AlgebricksException {
        for (ILogicalOperator li : leafInputs) {
            DataSourceScanOperator scanOp = (DataSourceScanOperator) findDataSourceScanOperator(li);
            if (scanOp == null)
                continue;
            Index index = joinEnum.getStatsHandle().findSampleIndex(scanOp, context);
            if (index == null) {
                return false;
            }
        }
        return true;
    }
}