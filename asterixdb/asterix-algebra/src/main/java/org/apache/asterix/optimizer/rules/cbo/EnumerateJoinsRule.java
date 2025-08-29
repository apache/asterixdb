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
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.IIndexProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.optimizer.rules.cbo.indexadvisor.AdvisorPlanParser;
import org.apache.asterix.optimizer.rules.cbo.indexadvisor.CBOPlanStateTree;
import org.apache.asterix.optimizer.rules.cbo.indexadvisor.FakeIndexProvider;
import org.apache.asterix.translator.SqlppExpressionToPlanTranslator;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.exceptions.ErrorCode;
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
    private List<Pair<ILogicalOperator, Integer>> parentsOfLeafInputs;
    private HashMap<LogicalVariable, Integer> varLeafInputIds;
    private List<Triple<Integer, Integer, Boolean>> buildSets; // the first is the bits and the second is the number of tables.
    private List<Quadruple<Integer, Integer, JoinOperator, Integer>> outerJoinsDependencyList;
    private List<AssignOperator> assignOps;
    private List<ILogicalExpression> assignJoinExprs; // These are the join expressions below the assign operator.

    // for the Array UNNEST optimization. The main list is for each leafInput.
    private List<List<List<ILogicalOperator>>> unnestOpsInfo;
    private boolean arrayUnnestPossible = false;
    // The Distinct operators for each DataSourceScan operator (if applicable)
    private HashMap<DataSourceScanOperator, ILogicalOperator> dataScanAndGroupByDistinctOps;

    // The Distinct/GroupBy operator at root of the query tree (if exists)
    private ILogicalOperator rootGroupByDistinctOp;

    // The OrderBy operator at root of the query tree (if exists)
    private ILogicalOperator rootOrderByOp;

    private List<LogicalVariable> resultAndJoinVars = new ArrayList();
    private final List<Boolean> realLeafInputs = new ArrayList();
    private int numberOfFromTerms;

    private List<Triple<ILogicalOperator, ILogicalOperator, List<ILogicalOperator>>> modifyUnnestInfo;
    private final Map<DataSourceScanOperator, Boolean> fakeLeafInputsMap = new HashMap();
    private ILogicalOperator newRootAfterUnnest = null;

    private IIndexProvider indexProvider;

    public EnumerateJoinsRule(JoinEnum joinEnum) {
        this.joinEnum = joinEnum;
        dataScanAndGroupByDistinctOps = new HashMap<>(); // initialized only once at the beginning of the rule
        rootGroupByDistinctOp = null;
        rootOrderByOp = null;
    }

    private void setIndexProvider(IIndexProvider indexProvider) {
        this.indexProvider = indexProvider;
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

        boolean cboMode = getCBOMode(context);
        boolean cboTestMode = getCBOTestMode(context);

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

        boolean adviseIndex = context.getIndexAdvisor().getAdvise();
        if (adviseIndex) {
            CBOPlanStateTree cboPlanStateTree = AdvisorPlanParser.getCBOPlanState(opRef, context);
            FakeIndexProvider fakeIndexProvider = new FakeIndexProvider(cboPlanStateTree);
            context.getIndexAdvisor().setFakeIndexProvider(fakeIndexProvider);
            setIndexProvider(fakeIndexProvider);
        } else {
            setIndexProvider((IIndexProvider) context.getMetadataProvider());
        }

        IPlanPrettyPrinter pp = context.getPrettyPrinter();
        String viewInPlan;
        if (LOGGER.isTraceEnabled()) {
            viewInPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
        }
        printPlan(pp, (AbstractLogicalOperator) op, "Original Whole plan1");

        int phase = 1;
        init(phase);
        boolean canTransform = getJoinOpsAndLeafInputs(null, op, -1, phase);

        if (!canTransform) {
            return cleanUp();
        }

        if (everyLeafInputDoesNotHaveADataScanOperator(leafInputs)) {
            return cleanUp();
        }
        if (LOGGER.isTraceEnabled()) {
            viewInPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
        }
        if (arrayUnnestPossible) {
            joinEnum.stats = new Stats(context, joinEnum);
            if (cboMode) {
                if (!doAllDataSourcesHaveSamples(leafInputs, context)) {
                    if (adviseIndex) {
                        errorOutIndexAdvisorSamplesNotFound(leafInputs, context);
                    }
                    return cleanUp();
                }
            }
            // Here on, we expect that changes can be made to the incoming plan and that optimization will proceed
            // without any hitch. Basically, we cannot go back now!!
            // now that we know it is safe to proceed with unnesting array optimization, we will remove
            // the unnestOps and related assign ops from the leafInputs and add them back later at the right places.
            //select count (*) from KS1 x, x.uarr_i, x.zarr_i, KS2 y, y. earr_i where x.rand_n = y.rand_n;
            //realInput 0 = true      (KS1)
            //realInput 1 = false
            //realInput 2 = false
            //realInput 3 = true (KS2)
            //realInput 4 = false
            //Note: The Unnesting code may move UNNEST Ops from the leafInputs higher up in the plan.
            //The code is not designed to deal with UNNEST Ops that are not in the leafInputs.
            int i = -1;
            int j = -1;
            for (List<List<ILogicalOperator>> l : unnestOpsInfo) {
                i++;
                if (realLeafInputs.get(i)) {
                    j++;
                    removeUnnestOpsFromLeafInputLevel1(leafInputs.get(j), l);
                }
            }

            // now the plan should have no unnestOps and no related assigns
            if (LOGGER.isTraceEnabled()) {
                String viewOldPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
            }
            introduceFakeOuterJoins(opRef, context);
            if (LOGGER.isTraceEnabled()) {
                String viewNewPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
            }
            phase = 2;
            init(phase);
            getJoinOpsAndLeafInputs(null, op, -1, phase);
        } else {
            unnestOpsInfo.clear();
        }

        collectJoinConditionsVariables(); // will be used for determining which variables will be projected from the base levels

        convertOuterJoinstoJoinsIfPossible(outerJoinsDependencyList);

        printPlan(pp, (AbstractLogicalOperator) op, "Original Whole plan2");
        numberOfFromTerms = leafInputs.size();

        if (LOGGER.isTraceEnabled()) {
            viewInPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
            LOGGER.trace("viewInPlan");
            LOGGER.trace(viewInPlan);
        }

        if (buildSets.size() > 1) {
            buildSets.sort(Comparator.comparingDouble(o -> o.second)); // sort on the number of tables in each set
            // we need to build the smaller sets first. So we need to find these first.
        }
        joinEnum.initEnum((AbstractLogicalOperator) op, cboMode, cboTestMode, numberOfFromTerms, leafInputs, allJoinOps,
                assignOps, outerJoinsDependencyList, buildSets, varLeafInputIds, unnestOpsInfo,
                dataScanAndGroupByDistinctOps, rootGroupByDistinctOp, rootOrderByOp, resultAndJoinVars,
                fakeLeafInputsMap, context, indexProvider);
        if (cboMode) {
            if (!doAllDataSourcesHaveSamples(leafInputs, context)) {
                if (adviseIndex) {
                    errorOutIndexAdvisorSamplesNotFound(leafInputs, context);
                }
                return cleanUp();
            }
        }
        if (LOGGER.isTraceEnabled()) {
            viewInPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
        }

        printLeafPlans(pp, leafInputs, "Inputs1");

        if (assignOps.size() > 0) {
            pushAssignsIntoLeafInputs(pp, leafInputs, assignOps, assignJoinExprs);
        }

        if (LOGGER.isTraceEnabled()) {
            viewInPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
        }
        printLeafPlans(pp, leafInputs, "Inputs2");
        if (LOGGER.isTraceEnabled()) {
            String viewPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
        }
        int cheapestPlan = joinEnum.enumerateJoins(); // MAIN CALL INTO CBO
        if (cheapestPlan == PlanNode.NO_PLAN) {
            return cleanUp();
        }

        PlanNode cheapestPlanNode = joinEnum.allPlans.get(cheapestPlan);

        generateHintWarnings();
        ILogicalOperator root = op;
        if (numberOfFromTerms > 1) {
            getNewJoinOps(cheapestPlanNode, allJoinOps);
            if (allJoinOps.size() != newJoinOps.size()) {
                return cleanUp(); // there are some cases such as R OJ S on true. Here there is an OJ predicate but the code in findJoinConditions
                // in JoinEnum does not capture this. Will fix later. Just bail for now.
            }
            if (LOGGER.isTraceEnabled()) {
                String viewInPlan2 = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
            }
            buildNewTree(cheapestPlanNode, newJoinOps, new MutableInt(0), context);
            root = newJoinOps.get(0);
            if (LOGGER.isTraceEnabled()) {
                String viewInPlan2 = new ALogicalPlanImpl(new MutableObject<>(root)).toString();
            }
            if (phase == 2) {
                // Now remove the Fake outer joins and put in the original Unnest Ops along with the corresponding Assign Ops
                modifyUnnestInfo = new ArrayList<>();
                collectUnnestModificationInfo(null, root, cheapestPlanNode);
                for (int k = 0; k < modifyUnnestInfo.size(); k++) {
                    modifyTree(null, root, k);
                    if (newRootAfterUnnest != null) {
                        root = newRootAfterUnnest;
                    }
                }
                Mutable<ILogicalOperator> rootRef = new MutableObject<>(root);
                if (LOGGER.isTraceEnabled()) {
                    String viewInPlan2 = new ALogicalPlanImpl(rootRef).toString(); //useful when debugging
                }
            }
            opRef.setValue(root);
            if (LOGGER.isTraceEnabled()) {
                String viewInPlan2 = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
            }
            context.computeAndSetTypeEnvironmentForOperator(root);

            if (assignOps.size() > 0) {
                for (int i = assignOps.size() - 1; i >= 0; i--) {
                    MutableBoolean removed = new MutableBoolean(false);
                    removed.setFalse();
                    pushAssignsAboveJoins(root, assignOps.get(i), assignJoinExprs.get(i), removed);
                    context.computeAndSetTypeEnvironmentForOperator(assignOps.get(i));
                    if (removed.isTrue()) {
                        assignOps.remove(i);
                    }
                }
            }

            printPlan(pp, (AbstractLogicalOperator) root, "New Whole Plan after buildNewTree 1");
            root = addRemainingAssignsAtTheTop(root, assignOps);
            printPlan(pp, (AbstractLogicalOperator) root, "New Whole Plan after buildNewTree 2");
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
                printPlan(pp, (AbstractLogicalOperator) root, "New Whole Plan");
            }
            // turn of this rule for all joins in this set (subtree)
            for (ILogicalOperator joinOp : newJoinOps) {
                context.addToDontApplySet(this, joinOp);
            }
        } else {
            buildNewTree(cheapestPlanNode);
        }
        context.computeAndSetTypeEnvironmentForOperator(root);
        if (LOGGER.isTraceEnabled()) {
            String finalPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
        }
        return true;
    }

    private boolean everyLeafInputDoesNotHaveADataScanOperator(List<ILogicalOperator> leafInputs) {
        for (ILogicalOperator leafInput : leafInputs) {
            DataSourceScanOperator scanOp = joinEnum.findDataSourceScanOperator(leafInput);
            if (scanOp == null) {
                return true;
            }
        }
        return false;
    }

    private boolean cleanUp() {
        removeTrueFromAllLeafInputs();
        return false;
    }

    private void modifyTree(ILogicalOperator parent, ILogicalOperator op, int k) {
        if (modifyUnnestInfo.get(k).second == op) { // found the one to get rid off; this should be an OJ
            int size = modifyUnnestInfo.get(k).third.size();
            UnnestOperator uOp = (UnnestOperator) modifyUnnestInfo.get(k).third.get(size - 1); // UnnestOp is always at the end
            uOp.getInputs().get(0).setValue(op.getInputs().get(0).getValue()); //The unnestOp is at the lowest position and points to the op below it.
            if (parent == null) { // change is happening at the very top, so the root will change
                newRootAfterUnnest = modifyUnnestInfo.get(k).third.get(0); // the first assign that belongs to this unnestOp. Or an only one UnnestOp with no assigns
                ILogicalOperator q = newRootAfterUnnest;
                if (modifyUnnestInfo.get(k).third.size() > 1) {
                    for (ILogicalOperator p : modifyUnnestInfo.get(k).third) {
                        q.getInputs().get(0).setValue(p);
                        q = p;
                    }
                }
            } else {
                ILogicalOperator q = parent;
                for (ILogicalOperator p : modifyUnnestInfo.get(k).third) {
                    q.getInputs().get(0).setValue(p);
                    q = p;
                }
            }
        }
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            modifyTree(op, input.getValue(), k);
        }
    }

    // This is a complicated routine. Removes unnestOperations from leafInputs. They will be added back at the right places.
    // Replaces fakeOuterJoins with Unnest Operations
    // The idea is very simple. We replace an UnnestOp (input a) with a LOJ (inputs a, fake datasource Scan Op b).
    // This goes to CBO.  When CBO returns, the LOJ's move around.
    // We replace the LOJ (input x, input b) with the UnnestOp (x)
    private void collectUnnestModificationInfo(ILogicalOperator parent, ILogicalOperator op, PlanNode plan) {
        // We cant go by the old leafInputs and parent structures, since the leafInputs may be in different places now.

        if (joinClause(op)) {
            PlanNode left = plan.getLeftPlanNode();
            PlanNode right = plan.getRightPlanNode();
            int rightjnNum = plan.getRightJoinIndex();
            JoinNode rightjn = joinEnum.jnArray[rightjnNum];
            if (rightjn.getFake()) {
                int leafInputNumber = rightjn.getLeafInputNumber();
                int arrayRef = rightjn.getArrayRef();
                modifyUnnestInfo
                        .add(new Triple<>(parent, op, unnestOpsInfo.get(leafInputNumber - 1).get(arrayRef - 1)));
            }
            parent = op;
            collectUnnestModificationInfo(parent, op.getInputs().get(0).getValue(), left);
            collectUnnestModificationInfo(parent, op.getInputs().get(1).getValue(), right);
        }
    }

    // create one fake outer join for each unnest operation;
    private void introduceFakeOuterJoins(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        int i = -1;
        int j = -1;

        for (List<List<ILogicalOperator>> l1 : unnestOpsInfo) {// each loop here is for a particular leafInout
            i++;
            if (realLeafInputs.get(i)) {
                j++;
            }
            if (l1.size() == 0) {
                continue;
            }

            LeftOuterJoinOperator foj = null;
            ILogicalOperator leftChild = leafInputs.get(j);
            ILogicalOperator leafInput = leafInputs.get(j);
            // We will add as many left outer joins as there are elements in l1
            // We will not modify leafInput. We will do that before issuing sampling queries. REMOVE
            ILogicalOperator parentOp = null; // The final left outerjoin operator is what we will attach the leafInput to

            for (List<ILogicalOperator> l2 : l1) {
                if (LOGGER.isTraceEnabled()) {
                    String viewInPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
                }
                DataSourceScanOperator fakeDs = (DataSourceScanOperator) truncateInput(leafInput);
                fakeLeafInputsMap.put(fakeDs, true);
                LogicalVariable var1 = fakeDs.getVariables().get(0);
                varLeafInputIds.put(var1, j);
                MutableObject<ILogicalOperator> q = new MutableObject<>(fakeDs);
                LogicalVariable var2 = modify(q.getValue(), context); // so as to make it fake, remove teh original variables
                varLeafInputIds.put(var2, j + 1); // this InputId has to be different from j, which is a real leaf input.
                joinEnum.varLeafInputIds = varLeafInputIds; // this is needed for making new join expressions
                ILogicalExpression expr = joinEnum.makeNewEQJoinExpr(var1, var2);
                foj = new LeftOuterJoinOperator(new MutableObject<>(expr), new MutableObject<>(leftChild), q,
                        ConstantExpression.MISSING.getValue());
                if (LOGGER.isTraceEnabled()) {
                    String viewInPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
                }
                leftChild = foj;
                if (LOGGER.isTraceEnabled()) {
                    String viewInPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
                }
            }
            Pair<ILogicalOperator, Integer> parent = parentsOfLeafInputs.get(j);
            parent.first.getInputs().get(parent.second).setValue(foj);
            if (LOGGER.isTraceEnabled()) {
                String viewInPlan = new ALogicalPlanImpl(opRef).toString(); //useful when debugging
            }
        }
    }

    // remove the old variables and add a new variable.
    private LogicalVariable modify(ILogicalOperator op, IOptimizationContext context) {
        DataSourceScanOperator dsOp = (DataSourceScanOperator) op;
        int s = dsOp.getVariables().size();
        dsOp.getVariables().clear();
        for (int i = 0; i < s; i++) {
            LogicalVariable newVar = context.newVar(); // need all three for alias stuff
            dsOp.getVariables().add(newVar);
        }
        return dsOp.getVariables().get(0);
    }

    private ILogicalOperator truncateInput(ILogicalOperator op) throws AlgebricksException {
        ILogicalOperator dsOp = joinEnum.findDataSourceScanOperator(op);
        ILogicalOperator ds = OperatorManipulationUtil.bottomUpCopyOperators(dsOp);
        return ds;
    }

    private void init(int phase) {
        allJoinOps = new ArrayList<>();
        newJoinOps = new ArrayList<>();
        leafInputs = new ArrayList<>();
        varLeafInputIds = new HashMap<>();
        if (phase == 1)
            unnestOpsInfo = new ArrayList<>();
        outerJoinsDependencyList = new ArrayList<>();
        parentsOfLeafInputs = new ArrayList<>();
        assignOps = new ArrayList<>();
        assignJoinExprs = new ArrayList<>();
        buildSets = new ArrayList<>();
        leafInputNumber = 0;
    }

    private void findUnnestOps(ILogicalOperator leafInput) throws AlgebricksException {
        ILogicalOperator p = leafInput;
        realLeafInputs.add(true); // every leafInput wil be real
        List<ILogicalOperator> unnestOps = findAllUnnestOps(p); // how many and which ones
        if (unnestOps.isEmpty()) {
            unnestOpsInfo.add(new ArrayList<>());
            return;
        }
        List<List<ILogicalOperator>> bigList = new ArrayList<>();
        int count = 0;
        for (ILogicalOperator op : unnestOps) {
            UnnestOperator unnestOp = (UnnestOperator) op;
            if (anyVarIsAJoinVar(unnestOp.getVariables())) {
                unnestOpsInfo.add(new ArrayList<>()); // each leafInput must have one entry
                //not possible to unnest this unnest op. If these variables participate in join predicates, then unnestOps cannot be moved above joins
                continue;
            }

            Pair<Boolean, List<ILogicalOperator>> info = findAllAssociatedAssingOps(p, unnestOp);
            if (!info.first) {// something 'bad' happened, so this unnestOp cannot be unnested
                unnestOpsInfo.add(new ArrayList<>()); // each leafInput must have one entry
                //not possible to unnest this unnest op. If these variables participate in join predicates, then unnestOps cannot be moved above joins
                continue;
            }
            count++; // found something unnestable
            bigList.add(info.second);
        }
        if (count > 0) {
            arrayUnnestPossible = true;
            unnestOpsInfo.add(bigList);
            for (int i = 0; i < count; i++) {
                unnestOpsInfo.add(new ArrayList<>()); // this is for the fake leaf Input that will be added for every unnestOp
                realLeafInputs.add(false);
            }
        }
    }

    Pair<Boolean, List<ILogicalOperator>> findAllAssociatedAssingOps(ILogicalOperator leafInput,
            UnnestOperator unnestOp) throws AlgebricksException {
        Pair<Boolean, List<ILogicalOperator>> info = new Pair<>(true, new ArrayList<>());
        ILogicalOperator p = leafInput;

        List<ILogicalOperator> ops = new ArrayList<>(); //Gather all AssignsOps, if any, associated wth this unnestOp
        while (p != null && p.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (p.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator aOp = (AssignOperator) p;

                ILogicalExpression a = aOp.getExpressions().get(0).getValue();
                if (a.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression exp =
                            (AbstractFunctionCallExpression) aOp.getExpressions().get(0).getValue();
                    if (exp.getKind() == AbstractFunctionCallExpression.FunctionKind.SCALAR) {
                        ILogicalExpression lexp = exp.getArguments().get(0).getValue();
                        if (lexp.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                            VariableReferenceExpression varRef = (VariableReferenceExpression) lexp;
                            LogicalVariable var = varRef.getVariableReference();
                            LogicalVariable unnestVar = unnestOp.getVariables().get(0);
                            if (unnestVar == var) {
                                if ((anyVarIsAJoinVar(aOp.getVariables())
                                        || assignVarPresentInLeafInput(aOp, leafInput))) {
                                    info.first = false;
                                    return info;
                                } else {
                                    ops.add(aOp);
                                }
                            }
                        }
                    }
                }
            }
            p = p.getInputs().get(0).getValue();
        }
        ops.add(unnestOp); // the unnestOp will be the last (and may be the only op)
        info.second = ops;

        /*
        unnestOpsInfo.add(bigList); // one for each LeafInput. If empty, means that there are no array references in this leafInout
        // also need to add some dummy entries for the fake leafInputs. Add as many as unnestOps. This will make the code in setCardsAndSizes happy
         */
        return info;
    }

    private boolean assignVarPresentInLeafInput(AssignOperator aOp, ILogicalOperator leafInput)
            throws AlgebricksException {
        List<LogicalVariable> vars = new ArrayList<>();
        for (LogicalVariable var : aOp.getVariables()) {
            ILogicalOperator p = leafInput;
            while (true) {
                vars.clear();
                VariableUtilities.getUsedVariables(p, vars);
                if (vars.contains(var)) {
                    return true;
                }
                if (p == aOp) { // No need to go below the assignOp
                    break;
                }
                p = p.getInputs().get(0).getValue();
            }
        }
        return false;
    }

    private boolean anyVarIsAJoinVar(List<LogicalVariable> vars) {
        for (LogicalVariable var : vars) {
            if (varIsAJoinVar(var)) {
                return true;
            }
        }
        return false;
    }

    private boolean varIsAJoinVar(LogicalVariable var) {
        for (JoinOperator j : allJoinOps) {
            List<LogicalVariable> joinExprVars = new ArrayList<>();
            AbstractBinaryJoinOperator jo = j.getAbstractJoinOp();
            ILogicalExpression expr = jo.getCondition().getValue();
            joinExprVars.clear();
            expr.getUsedVariables(joinExprVars);
            for (LogicalVariable lv : joinExprVars) {
                if (lv.equals(var)) {
                    return true;
                }
            }
        }
        return false;
    }

    // Guessing this is inefficient but do not expect leafInputs to be huge, so efficiency may not be a concern
    private static void removeUnnestOpsFromLeafInputLevel1(ILogicalOperator leafInput,
            List<List<ILogicalOperator>> bigList) {
        for (List<ILogicalOperator> l : bigList) {
            removeUnnestOpsFromLeafInputLevel2(leafInput, l);
        }
    }

    private static void removeUnnestOpsFromLeafInputLevel2(ILogicalOperator leafInput, List<ILogicalOperator> list) {
        for (ILogicalOperator op : list) {
            removeUnnestOpFromLeafInputLevel3(leafInput, op);
        }
    }

    private static void removeUnnestOpFromLeafInputLevel3(ILogicalOperator leafInput, ILogicalOperator op) {
        ILogicalOperator parent = leafInput; // always a select Op with condition true
        ILogicalOperator p = leafInput.getInputs().get(0).getValue();

        while (p != null && p.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (p == op) {
                parent.getInputs().get(0).setValue(op.getInputs().get(0).getValue());
            }
            parent = p;
            p = p.getInputs().get(0).getValue();
        }
    }

    private List<ILogicalOperator> findAllUnnestOps(ILogicalOperator p) {
        List<ILogicalOperator> list = new ArrayList<>();
        while (p != null && p.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (p.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                UnnestOperator unnestOp = (UnnestOperator) p;
                Object arrayAccess = unnestOp.getAnnotations().get(SqlppExpressionToPlanTranslator.ARRAY_ACCESS);
                if (arrayAccess != null && (boolean) arrayAccess) {
                    list.add(p);
                }
            }
            p = p.getInputs().get(0).getValue();
        }
        return list;
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

    public static boolean joinClause(ILogicalOperator op) {
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

    public static boolean getCBOMode(IOptimizationContext context) {
        PhysicalOptimizationConfig physOptConfig = context.getPhysicalOptimizationConfig();
        return physOptConfig.getCBOMode();
    }

    public static boolean getCBOTestMode(IOptimizationContext context) {
        PhysicalOptimizationConfig physOptConfig = context.getPhysicalOptimizationConfig();
        return physOptConfig.getCBOTestMode();
    }

    /**
     * Should not see any kind of joins here. store the emptyTupeSourceOp and DataSource operators.
     * Each leaf input will normally have both, but sometimes only emptyTupeSourceOp will be present (in lists)
     */
    public static Pair<EmptyTupleSourceOperator, DataSourceScanOperator> containsLeafInputOnly(ILogicalOperator op) {
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
    public static int numVarRefExprs(AssignOperator aOp) {
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

    public static ILogicalOperator skipPastAssigns(ILogicalOperator nextOp) {
        while (nextOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            nextOp = nextOp.getInputs().get(0).getValue();
        }
        return nextOp;
    }

    private ILogicalOperator findSelectOrUnnestOrDataScan(ILogicalOperator op) {
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
            if ((tag == LogicalOperatorTag.SELECT) || (tag == LogicalOperatorTag.UNNEST)
                    || (tag == LogicalOperatorTag.DATASOURCESCAN)) {
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
    private void buildDependencyList(ILogicalOperator op, JoinOperator jO,
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
    private boolean getJoinOpsAndLeafInputs(ILogicalOperator parent, ILogicalOperator op, int leftRight, int phase)
            throws AlgebricksException {
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
                boolean canTransform = getJoinOpsAndLeafInputs(op, nextOp, i, phase);
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
                    buildDependencyList(op, jO, outerJoinsDependencyList, k);
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
                    ILogicalOperator selectOp = findSelectOrUnnestOrDataScan(op);
                    if (selectOp == null) {
                        return false;
                    } else {
                        leafInputs.add(selectOp);
                    }
                } else {
                    leafInputNumber++;
                    // always add a SelectOperator with TRUE condition. The code below becomes simpler with a select operator.
                    // We will always have a parent op, so we can remove the operator below this selOp without affecting the leafInput
                    if (phase == 1) { // dont want to add two of these
                        SelectOperator selOp = new SelectOperator(new MutableObject<>(ConstantExpression.TRUE));
                        parent.getInputs().get(leftRight).setValue(selOp);
                        selOp.getInputs().add(new MutableObject<>(null)); //add an input
                        selOp.getInputs().get(0).setValue(op);
                        leafInputs.add(selOp);
                        findUnnestOps(selOp);
                    } else {
                        leafInputs.add(op);
                    }
                    parentsOfLeafInputs.add(new Pair<>(parent, leftRight));
                    if (!addLeafInputNumbersToVars(op)) {
                        return false;
                    }
                }
            } else { // This must be an internal edge
                if (onlyAssigns(op, assignOps)) {
                    ILogicalOperator skipAssisgnsOp = skipPastAssigns(op);
                    boolean canTransform = getJoinOpsAndLeafInputs(op, skipAssisgnsOp, leftRight, phase);
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
        op.getAnnotations().put(OperatorAnnotations.OP_INPUT_DOCSIZE,
                (double) Math.round(plan.getJoinNode().getInputSize() * 100) / 100);
        op.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_DOCSIZE,
                (double) Math.round(plan.getJoinNode().getOutputSize() * 100) / 100);
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
    /*
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
    
     */

    private void removeJoinAnnotations(AbstractFunctionCallExpression afcExpr) {
        afcExpr.removeAnnotation(BroadcastExpressionAnnotation.class);
        afcExpr.removeAnnotation(IndexedNLJoinExpressionAnnotation.class);
        afcExpr.removeAnnotation(HashJoinExpressionAnnotation.class);
    }

    protected static void setAnnotation(AbstractFunctionCallExpression afcExpr, IExpressionAnnotation anno) {
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
        ILogicalOperator selOp = findSelectOrUnnestOrDataScan(leftInput);
        if (selOp != null) {
            addCardCostAnnotations(selOp, plan);
        }
        addCardCostAnnotations(joinEnum.findDataSourceScanOperator(leftInput), plan);
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
            if (plan.joinHint != null) {
                setAnnotation(afcExpr, plan.joinHint);
            } else {
                setAnnotation(plan.exprAndHint.first, plan.exprAndHint.second);
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Added IndexedNLJoinExpressionAnnotation to " + afcExpr.toString());
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
        PlanNode leftPlan = allPlans.get(leftIndex);
        PlanNode rightPlan = allPlans.get(rightIndex);

        ILogicalOperator joinOp = joinOps.get(totalNumberOfJoins.intValue()); // intValue set to 0 initially

        if (plan.IsJoinNode()) {
            fillJoinAnnotations(plan, joinOp);
        }

        if (leftPlan.IsScanNode()) {
            // leaf
            ILogicalOperator leftInput = removeTrue(leftPlan.getLeafInput());
            skipAllIndexes(leftPlan, leftInput);
            ILogicalOperator selOp = findSelectOrUnnestOrDataScan(leftInput);
            if (selOp != null) {
                addCardCostAnnotations(selOp, leftPlan);
            }
            joinOp.getInputs().get(0).setValue(leftInput);
            ILogicalOperator op = joinOp.getInputs().get(0).getValue();
            context.computeAndSetTypeEnvironmentForOperator(op);
            addCardCostAnnotations(joinEnum.findDataSourceScanOperator(leftInput), leftPlan);
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
            ILogicalOperator rightInput = removeTrue(rightPlan.getLeafInput());
            skipAllIndexes(rightPlan, rightInput);
            ILogicalOperator selOp = findSelectOrUnnestOrDataScan(rightInput);
            if (selOp != null) {
                addCardCostAnnotations(selOp, rightPlan);
            }
            joinOp.getInputs().get(1).setValue(rightInput);
            context.computeAndSetTypeEnvironmentForOperator(joinOp.getInputs().get(1).getValue());
            addCardCostAnnotations(joinEnum.findDataSourceScanOperator(rightInput), rightPlan);
        } else {
            // join
            totalNumberOfJoins.increment();
            ILogicalOperator rightInput = joinOps.get(totalNumberOfJoins.intValue());
            joinOp.getInputs().get(1).setValue(rightInput);
            context.computeAndSetTypeEnvironmentForOperator(joinOp.getInputs().get(1).getValue());
            buildNewTree(rightPlan, joinOps, totalNumberOfJoins, context);
        }
    }

    static ILogicalOperator removeTrue(ILogicalOperator leafInput) {
        if (leafInput.getOperatorTag() == LogicalOperatorTag.SELECT) {
            SelectOperator selOp = (SelectOperator) leafInput;
            if (selOp.getCondition().getValue() == ConstantExpression.TRUE) {
                return leafInput.getInputs().get(0).getValue();
            }
        }
        return leafInput;
    }

    // remove any selectops that may have been added in phase1
    private void removeTrueFromAllLeafInputs() {
        for (Pair<ILogicalOperator, Integer> parent : parentsOfLeafInputs) {
            ILogicalOperator nextOp = parent.getFirst().getInputs().get(parent.getSecond()).getValue();
            if (nextOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selOp = (SelectOperator) nextOp;
                if (selOp.getCondition().getValue() == ConstantExpression.TRUE) {
                    parent.getFirst().getInputs().get(parent.getSecond())
                            .setValue(nextOp.getInputs().get(0).getValue());
                }
            }
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
        int n = 0;
        for (ILogicalOperator li : leafInputs) {
            DataSourceScanOperator scanOp = joinEnum.findDataSourceScanOperator(li);
            if (scanOp == null) {
                continue;
            }
            Stats handle = joinEnum.getStatsHandle();
            if (handle == null) {
                continue;
            }
            Index index = handle.findSampleIndex(scanOp, context);
            if (index == null) {
                return false;
            }
            n++;
        }
        return (leafInputs.size() == n);
    }

    private void errorOutIndexAdvisorSamplesNotFound(List<ILogicalOperator> leafInputs, IOptimizationContext context)
            throws AlgebricksException {
        for (ILogicalOperator li : leafInputs) {
            DataSourceScanOperator scanOp = joinEnum.findDataSourceScanOperator(li);
            if (scanOp == null) {
                // Scan Operator not found
                continue;
            }
            Stats handle = joinEnum.getStatsHandle();
            if (handle == null) {
                continue;
            }
            Index index = handle.findSampleIndex(scanOp, context);
            if (index == null) {
                errorOutIndexAdvisorSampleNotFound(scanOp, context);
            }
        }
    }

    private void errorOutIndexAdvisorSampleNotFound(DataSourceScanOperator scanOperator, IOptimizationContext context)
            throws AlgebricksException {
        if (!(scanOperator.getDataSource() instanceof DatasetDataSource dataSource)) {
            return;
        }
        DatasetFullyQualifiedName fullyQualifiedName = dataSource.getDataset().getDatasetFullyQualifiedName();
        throw new AlgebricksException(ErrorCode.INDEX_ADVISOR_SAMPLE_NOT_FOUND,
                createSampleStatement(fullyQualifiedName));
    }

    private static String createSampleStatement(DatasetFullyQualifiedName dqn) {
        return "ANALYZE COLLECTION `" + dqn.getDatabaseName() + "`.`" + dqn.getDataverseName() + "`.`"
                + dqn.getDatasetName() + "`;";
    }

}
