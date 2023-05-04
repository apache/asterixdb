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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.annotations.IndexedNLJoinExpressionAnnotation;
import org.apache.asterix.common.annotations.SecondaryIndexSearchPreferenceAnnotation;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.declared.SampleDataSource;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.optimizer.cost.Cost;
import org.apache.asterix.optimizer.cost.CostMethods;
import org.apache.asterix.optimizer.cost.ICost;
import org.apache.asterix.optimizer.cost.ICostMethods;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.HashJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.PredicateCardinalityAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JoinEnum {

    private static final Logger LOGGER = LogManager.getLogger();

    protected List<JoinCondition> joinConditions; // "global" list of join conditions
    protected Map<IExpressionAnnotation, Warning> joinHints;
    protected List<PlanNode> allPlans; // list of all plans
    protected JoinNode[] jnArray; // array of all join nodes
    protected int jnArraySize;
    private List<Pair<EmptyTupleSourceOperator, DataSourceScanOperator>> emptyTupleAndDataSourceOps;
    protected Map<EmptyTupleSourceOperator, ILogicalOperator> joinLeafInputsHashMap;
    protected List<ILogicalExpression> singleDatasetPreds;
    private List<AssignOperator> assignOps;
    private List<ILogicalOperator> joinOps;
    protected ILogicalOperator localJoinOp; // used in nestedLoopsApplicable code.
    protected IOptimizationContext optCtx;

    protected Stats stats;
    private boolean cboMode;
    private boolean cboTestMode;
    protected int numberOfTerms;
    private AbstractLogicalOperator op;
    protected boolean connectedJoinGraph;
    protected boolean forceJoinOrderMode;
    protected String queryPlanShape;
    protected ICost cost;
    protected ICostMethods costMethods;

    public JoinEnum() {
    }

    protected void initEnum(AbstractLogicalOperator op, boolean cboMode, boolean cboTestMode, int numberOfFromTerms,
            List<Pair<EmptyTupleSourceOperator, DataSourceScanOperator>> emptyTupleAndDataSourceOps,
            Map<EmptyTupleSourceOperator, ILogicalOperator> joinLeafInputsHashMap, List<ILogicalOperator> joinOps,
            List<AssignOperator> assignOps, IOptimizationContext context) {
        this.singleDatasetPreds = new ArrayList<>();
        this.joinConditions = new ArrayList<>();
        this.joinHints = new HashMap<>();
        this.allPlans = new ArrayList<>();
        this.numberOfTerms = numberOfFromTerms;
        this.cboMode = cboMode;
        this.cboTestMode = cboTestMode;
        this.connectedJoinGraph = true;
        this.optCtx = context;
        this.emptyTupleAndDataSourceOps = emptyTupleAndDataSourceOps;
        this.joinLeafInputsHashMap = joinLeafInputsHashMap;
        this.assignOps = assignOps;
        this.joinOps = joinOps;
        this.op = op;
        this.forceJoinOrderMode = getForceJoinOrderMode(context);
        this.queryPlanShape = getQueryPlanShape(context);
        initCostHandleAndJoinNodes(context);
    }

    protected void initCostHandleAndJoinNodes(IOptimizationContext context) {
        this.cost = new Cost();
        this.costMethods = new CostMethods(context);
        this.stats = new Stats(optCtx, this);
        this.jnArraySize = (int) Math.pow(2.0, this.numberOfTerms);
        this.jnArray = new JoinNode[this.jnArraySize];
        // initialize all the join nodes
        for (int i = 0; i < this.jnArraySize; i++) {
            this.jnArray[i] = new JoinNode(i, this);
        }
    }

    protected List<JoinCondition> getJoinConditions() {
        return joinConditions;
    }

    public List<PlanNode> getAllPlans() {
        return allPlans;
    }

    protected JoinNode[] getJnArray() {
        return jnArray;
    }

    protected Cost getCostHandle() {
        return (Cost) cost;
    }

    protected CostMethods getCostMethodsHandle() {
        return (CostMethods) costMethods;
    }

    protected Stats getStatsHandle() {
        return stats;
    }

    protected ILogicalOperator findLeafInput(List<LogicalVariable> logicalVars) throws AlgebricksException {
        Set<LogicalVariable> vars = new HashSet<>();
        for (Pair<EmptyTupleSourceOperator, DataSourceScanOperator> emptyTupleAndDataSourceOp : emptyTupleAndDataSourceOps) {
            EmptyTupleSourceOperator emptyOp = emptyTupleAndDataSourceOp.getFirst();
            ILogicalOperator op = joinLeafInputsHashMap.get(emptyOp);
            vars.clear();
            // this is expensive to do. So store this once and reuse
            VariableUtilities.getLiveVariables(op, vars);
            if (vars.containsAll(logicalVars)) {
                return op;
            }
        }
        // this will never happen, but keep compiler happy
        return null;
    }

    protected ILogicalExpression combineAllConditions(List<Integer> newJoinConditions) {
        if (newJoinConditions.size() == 0) {
            // this is a cartesian product
            return ConstantExpression.TRUE;
        }
        if (newJoinConditions.size() == 1) {
            JoinCondition jc = joinConditions.get(newJoinConditions.get(0));
            return jc.joinCondition;
        }
        ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));

        for (int joinNum : newJoinConditions) {
            // Need to AND all the expressions.
            JoinCondition jc = joinConditions.get(joinNum);
            andExpr.getArguments().add(new MutableObject<>(jc.joinCondition));
        }
        return andExpr;
    }

    protected ILogicalExpression getNestedLoopJoinExpr(List<Integer> newJoinConditions) {
        if (newJoinConditions.size() != 1) {
            // may remove this restriction later if possible
            return null;
        }
        JoinCondition jc = joinConditions.get(newJoinConditions.get(0));
        return jc.joinCondition;
    }

    protected ILogicalExpression getHashJoinExpr(List<Integer> newJoinConditions) {
        if (newJoinConditions.size() == 0) {
            // this is a cartesian product
            return ConstantExpression.TRUE;
        }
        if (newJoinConditions.size() == 1) {
            JoinCondition jc = joinConditions.get(newJoinConditions.get(0));
            if (jc.comparisonType == JoinCondition.comparisonOp.OP_EQ) {
                return jc.joinCondition;
            }
            return null;
        }
        ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));

        // at least one equality predicate needs to be present for a hash join to be possible.
        boolean eqPredFound = false;
        for (int joinNum : newJoinConditions) {
            // need to AND all the expressions.
            JoinCondition jc = joinConditions.get(joinNum);
            if (jc.comparisonType == JoinCondition.comparisonOp.OP_EQ) {
                eqPredFound = true;
            }
            andExpr.getArguments().add(new MutableObject<>(jc.joinCondition));
        }
        // return null if no equality predicates were found
        return eqPredFound ? andExpr : null;
    }

    protected HashJoinExpressionAnnotation findHashJoinHint(List<Integer> newJoinConditions) {
        for (int i : newJoinConditions) {
            JoinCondition jc = joinConditions.get(i);
            if (jc.comparisonType != JoinCondition.comparisonOp.OP_EQ) {
                return null;
            }
            ILogicalExpression expr = jc.joinCondition;
            if (expr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                AbstractFunctionCallExpression AFCexpr = (AbstractFunctionCallExpression) expr;
                HashJoinExpressionAnnotation hjea = AFCexpr.getAnnotation(HashJoinExpressionAnnotation.class);
                if (hjea != null) {
                    return hjea;
                }
            }
        }
        return null;
    }

    protected BroadcastExpressionAnnotation findBroadcastHashJoinHint(List<Integer> newJoinConditions) {
        for (int i : newJoinConditions) {
            JoinCondition jc = joinConditions.get(i);
            if (jc.comparisonType != JoinCondition.comparisonOp.OP_EQ) {
                return null;
            }
            ILogicalExpression expr = jc.joinCondition;
            if (expr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                AbstractFunctionCallExpression AFCexpr = (AbstractFunctionCallExpression) expr;
                BroadcastExpressionAnnotation bcasthjea = AFCexpr.getAnnotation(BroadcastExpressionAnnotation.class);
                if (bcasthjea != null) {
                    return bcasthjea;
                }
            }
        }
        return null;
    }

    protected IndexedNLJoinExpressionAnnotation findNLJoinHint(List<Integer> newJoinConditions) {
        for (int i : newJoinConditions) {
            JoinCondition jc = joinConditions.get(i);
            ILogicalExpression expr = jc.joinCondition;
            if (expr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                AbstractFunctionCallExpression AFCexpr = (AbstractFunctionCallExpression) expr;
                IndexedNLJoinExpressionAnnotation inljea =
                        AFCexpr.getAnnotation(IndexedNLJoinExpressionAnnotation.class);
                if (inljea != null) {
                    return inljea;
                }
            }
        }
        return null;
    }

    public boolean findUseIndexHint(AbstractFunctionCallExpression condition) {
        if (condition.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND)) {
            for (int i = 0; i < condition.getArguments().size(); i++) {
                ILogicalExpression expr = condition.getArguments().get(i).getValue();
                if (expr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                    AbstractFunctionCallExpression AFCexpr = (AbstractFunctionCallExpression) expr;
                    if (AFCexpr.hasAnnotation(SecondaryIndexSearchPreferenceAnnotation.class)) {
                        return true;
                    }
                }
            }
        } else if (condition.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            if (condition.hasAnnotation(SecondaryIndexSearchPreferenceAnnotation.class)) {
                return true;
            }
        }
        return false;
    }

    protected int findJoinNodeIndexByName(String name) {
        for (int i = 1; i <= this.numberOfTerms; i++) {
            if (name.equals(jnArray[i].datasetNames.get(0))) {
                return i;
            } else if (name.equals(jnArray[i].aliases.get(0))) {
                return i;
            }
        }
        // should never happen; keep compiler happy.
        return JoinNode.NO_JN;
    }

    protected int findJoinNodeIndex(LogicalVariable lv) throws AlgebricksException {
        List<Pair<EmptyTupleSourceOperator, DataSourceScanOperator>> emptyTupleAndDataSourceOps =
                this.emptyTupleAndDataSourceOps;
        Map<EmptyTupleSourceOperator, ILogicalOperator> joinLeafInputsHashMap = this.joinLeafInputsHashMap;

        for (Map.Entry<EmptyTupleSourceOperator, ILogicalOperator> mapElement : joinLeafInputsHashMap.entrySet()) {
            ILogicalOperator joinLeafInput = mapElement.getValue();
            HashSet<LogicalVariable> vars = new HashSet<>();
            // this should get the variables from the inputs only, since the join condition is itself set to null
            VariableUtilities.getLiveVariables(joinLeafInput, vars);
            if (vars.contains(lv)) {
                EmptyTupleSourceOperator key = mapElement.getKey();
                for (int i = 0; i < emptyTupleAndDataSourceOps.size(); i++) {
                    if (key.equals(emptyTupleAndDataSourceOps.get(i).getFirst())) {
                        return i;
                    }
                }
            }
        }
        return JoinNode.NO_JN;
    }

    private int findBits(LogicalVariable lv) throws AlgebricksException {
        int idx = findJoinNodeIndex(lv);
        if (idx >= 0) {
            return 1 << idx;
        }

        // so this variable must be in an internal edge in an assign statement. Find the RHS variables there
        for (AssignOperator op : this.assignOps) {
            List<LogicalVariable> vars2 = new ArrayList<>();
            VariableUtilities.getUsedVariables(op, vars2);
            int bits = 0;
            for (LogicalVariable lv2 : vars2) {
                bits |= findBits(lv2);
            }
            return bits;
        }
        // should never reach this because every variable must exist in some leaf input.
        return JoinNode.NO_JN;
    }

    // This finds all the join Conditions in the whole query. This is a global list of all join predicates.
    // It also fills in the dataset Bits for each join predicate.
    private void findJoinConditionsAndAssignSels() throws AlgebricksException {
        List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
        for (ILogicalOperator jOp : joinOps) {
            AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) jOp;
            ILogicalExpression expr = joinOp.getCondition().getValue();
            conjs.clear();
            if (expr.splitIntoConjuncts(conjs)) {
                conjs.remove(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
                for (Mutable<ILogicalExpression> conj : conjs) {
                    JoinCondition jc = new JoinCondition();
                    jc.joinCondition = conj.getValue().cloneExpression();
                    joinConditions.add(jc);
                    jc.selectivity = stats.getSelectivityFromAnnotationMain(jc.joinCondition, true);
                }
            } else {
                if ((expr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL))) {
                    JoinCondition jc = new JoinCondition();
                    // change to not a true condition
                    jc.joinCondition = expr.cloneExpression();
                    joinConditions.add(jc);
                    jc.selectivity = stats.getSelectivityFromAnnotationMain(jc.joinCondition, true);
                }
            }
        }

        // now patch up any join conditions that have variables referenced in any internal assign statements.
        List<LogicalVariable> usedVars = new ArrayList<>();
        for (JoinCondition jc : joinConditions) {
            usedVars.clear();
            ILogicalExpression expr = jc.joinCondition;
            expr.getUsedVariables(usedVars);
            for (AssignOperator aOp : assignOps) {
                for (int i = 0; i < aOp.getVariables().size(); i++) {
                    if (usedVars.contains(aOp.getVariables().get(i))) {
                        OperatorManipulationUtil.replaceVarWithExpr((AbstractFunctionCallExpression) expr,
                                aOp.getVariables().get(i), aOp.getExpressions().get(i).getValue());
                        jc.joinCondition = expr;
                        jc.selectivity = stats.getSelectivityFromAnnotationMain(jc.joinCondition, true);
                    }
                }
            }
        }

        // now fill the datasetBits for each join condition.
        for (JoinCondition jc : joinConditions) {
            ILogicalExpression joinExpr = jc.joinCondition;
            usedVars.clear();
            joinExpr.getUsedVariables(usedVars);
            // We only set these for join predicates that have exactly two tables
            jc.leftSideBits = jc.rightSideBits = JoinCondition.NO_JC;
            if (((AbstractFunctionCallExpression) joinExpr).getFunctionIdentifier()
                    .equals(AlgebricksBuiltinFunctions.EQ)) {
                jc.comparisonType = JoinCondition.comparisonOp.OP_EQ;
            } else {
                jc.comparisonType = JoinCondition.comparisonOp.OP_OTHER;
            }
            jc.numberOfVars = usedVars.size();

            for (int i = 0; i < jc.numberOfVars; i++) {
                int bits = findBits(usedVars.get(i)); // rename to findInWhichLeaf
                if (bits != JoinCondition.NO_JC) {
                    if (i == 0) {
                        jc.leftSideBits = bits;
                    } else if (i == 1) {
                        jc.rightSideBits = bits;
                    } else {
                        // have to deal with preds such as r.a + s.a = 5 OR r.a + s.a = t.a
                    }
                    jc.datasetBits |= bits;
                }
            }
        }
    }

    // in case we have l.partkey = ps.partkey and l.suppkey = ps.suppkey, we will only use the first one for cardinality computations.
    // treat it like a Pk-Fk join; simplifies cardinality computation
    private void markCompositeJoinPredicates() {
        // can use dataSetBits??? This will be simpler.
        for (int i = 0; i < joinConditions.size() - 1; i++) {
            for (int j = i + 1; j < joinConditions.size(); j++) {
                if (joinConditions.get(i).datasetBits == joinConditions.get(j).datasetBits) {
                    joinConditions.get(j).partOfComposite = true;
                }
            }
        }
    }

    private boolean verticesMatch(JoinCondition jc1, JoinCondition jc2) {
        return jc1.leftSideBits == jc2.leftSideBits || jc1.leftSideBits == jc2.rightSideBits
                || jc1.rightSideBits == jc2.leftSideBits || jc1.rightSideBits == jc2.rightSideBits;
    }

    private void markComponents(int startingJoinCondition) {
        List<JoinCondition> joinConditions = this.getJoinConditions();
        // see if all the joinCondition can be reached starting with the first.
        JoinCondition jc1 = joinConditions.get(startingJoinCondition);
        for (int i = 0; i < joinConditions.size(); i++) {
            JoinCondition jc2 = joinConditions.get(i);
            if (i != startingJoinCondition && jc2.componentNumber == 0) {
                // a new edge not visited before
                if (verticesMatch(jc1, jc2)) {
                    jc2.componentNumber = 1;
                    markComponents(i);
                }
            }
        }
    }

    private void findIfJoinGraphIsConnected() {
        int numJoinConditions = joinConditions.size();
        if (numJoinConditions < numberOfTerms - 1) {
            /// not enough join predicates
            connectedJoinGraph = false;
            return;
        }
        if (numJoinConditions > 0) {
            joinConditions.get(0).componentNumber = 1;
            markComponents(0);
            for (int i = 1; i < numJoinConditions; i++) {
                if (joinConditions.get(i).componentNumber == 0) {
                    connectedJoinGraph = false;
                    return;
                }
            }
        }
    }

    private double findInListCard(ILogicalOperator op) {
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            return 1.0;
        }

        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST) {
            UnnestOperator unnestOp = (UnnestOperator) op;
            ILogicalExpression unnestExpr = unnestOp.getExpressionRef().getValue();
            UnnestingFunctionCallExpression unnestingFuncExpr = (UnnestingFunctionCallExpression) unnestExpr;

            if (unnestingFuncExpr.getFunctionIdentifier().equals(BuiltinFunctions.SCAN_COLLECTION)) {
                if (unnestingFuncExpr.getArguments().get(0).getValue()
                        .getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    ConstantExpression constantExpr =
                            (ConstantExpression) unnestingFuncExpr.getArguments().get(0).getValue();
                    AsterixConstantValue constantValue = (AsterixConstantValue) constantExpr.getValue();
                    IAObject v = constantValue.getObject();
                    if (v.getType().getTypeTag() == ATypeTag.ARRAY) {
                        AOrderedList array = (AOrderedList) v;
                        return array.size();
                    }
                }
            }
        }
        // just a guess
        return 10.0;
    }

    private String findAlias(DataSourceScanOperator scanOp) {
        DataSource ds = (DataSource) scanOp.getDataSource();
        List<LogicalVariable> allVars = scanOp.getVariables();
        LogicalVariable dataRecVarInScan = ds.getDataRecordVariable(allVars);
        return dataRecVarInScan.toString().substring(2);
    }

    private int addNonBushyJoinNodes(int level, int jnNumber, int[] startJnAtLevel) throws AlgebricksException {
        // adding joinNodes of level (2, 3, ..., numberOfTerms)
        int startJnSecondLevel = startJnAtLevel[2];
        int startJnPrevLevel = startJnAtLevel[level - 1];
        int startJnNextLevel = startJnAtLevel[level];
        int i, j, addPlansToThisJn;

        // walking thru the previous level
        for (i = startJnPrevLevel; i < startJnNextLevel; i++) {
            JoinNode jnI = jnArray[i];
            jnI.jnArrayIndex = i;
            if (jnI.highestDatasetId == 0) {
                // this jn can be skipped
                continue;
            }

            // walk thru the first level here
            for (j = 1; j < startJnSecondLevel; j++) {
                if (level == 2 && i > j) {
                    // don't want to generate x y and y x. we will do this in plan generation.
                    continue;
                }
                JoinNode jnJ = jnArray[j];
                jnJ.jnArrayIndex = j;
                if ((jnI.datasetBits & jnJ.datasetBits) > 0) {
                    // these already have some common table
                    continue;
                }
                int newBits = jnI.datasetBits | jnJ.datasetBits;
                JoinNode jnNewBits = jnArray[newBits];
                jnNewBits.jnArrayIndex = newBits;
                // visiting this join node for the first time
                if (jnNewBits.jnIndex == 0) {
                    jnNumber++;
                    JoinNode jn = jnArray[jnNumber];
                    jn.jnArrayIndex = jnNumber;
                    // if we want to locate the joinNode num (say 33) which has tables 1, 2, and 5.
                    // if these bits are turned on, we get 19. Then jn[19].jn_index will equal 33.
                    // Then jn[33].highestKeyspaceId will equal 5
                    // if this joinNode ever gets removed, then set jn[19].highestKeyspaceId = 0
                    jn.datasetBits = newBits;
                    jnNewBits.jnIndex = addPlansToThisJn = jnNumber;
                    jn.level = level;
                    jn.highestDatasetId = Math.max(jnI.highestDatasetId, j);

                    jn.datasetIndexes = new ArrayList<>();
                    jn.datasetIndexes.addAll(jnI.datasetIndexes);
                    jn.datasetIndexes.addAll(jnJ.datasetIndexes);
                    Collections.sort(jn.datasetIndexes);

                    jn.datasetNames = new ArrayList<>();
                    jn.datasetNames.addAll(jnI.datasetNames);
                    jn.datasetNames.addAll(jnJ.datasetNames);
                    Collections.sort(jn.datasetNames);
                    jn.aliases = new ArrayList<>();
                    jn.aliases.addAll(jnI.aliases);
                    jn.aliases.addAll(jnJ.aliases);
                    Collections.sort(jn.aliases);
                    jn.size = jnI.size + jnJ.size;
                    jn.cardinality = jn.computeJoinCardinality();
                    if (jn.cardinality < 2.1) {
                        jn.cardinality = 2.1; // for keeping CP and HJ cost formulas happy.
                    }
                } else {
                    addPlansToThisJn = jnNewBits.jnIndex;
                }

                JoinNode jnIJ = jnArray[addPlansToThisJn];
                jnIJ.jnArrayIndex = addPlansToThisJn;
                jnIJ.addMultiDatasetPlans(jnI, jnJ);
                if (forceJoinOrderMode) {
                    break;
                }
            }
            if (forceJoinOrderMode) {
                break;
            }
        }

        return jnNumber;
    }

    private int enumerateHigherLevelJoinNodes() throws AlgebricksException {
        int jnNumber = this.numberOfTerms;
        int[] firstJnAtLevel;
        firstJnAtLevel = new int[numberOfTerms + 1];
        firstJnAtLevel[1] = 1;
        IPlanPrettyPrinter pp = optCtx.getPrettyPrinter();
        // after implementing greedy plan, we can start at level 3;
        int startLevel = 2;
        if (LOGGER.isTraceEnabled()) {
            EnumerateJoinsRule.printPlan(pp, op, "Original Whole plan in JN 4");
        }
        for (int level = startLevel; level <= numberOfTerms; level++) {
            firstJnAtLevel[level] = jnNumber + 1;
            jnNumber = addNonBushyJoinNodes(level, jnNumber, firstJnAtLevel);
        }
        if (LOGGER.isTraceEnabled()) {
            EnumerateJoinsRule.printPlan(pp, op, "Original Whole plan in JN 5");
        }
        return jnNumber;
    }

    private int initializeBaseLevelJoinNodes() throws AlgebricksException {
        // join nodes have been allocated in the JoinEnum
        // add a dummy Plan Node; we do not want planNode at position 0 to be a valid plan
        PlanNode pn = new PlanNode(0, this);
        pn.jn = null;
        pn.jnIndexes[0] = pn.jnIndexes[1] = JoinNode.NO_JN;
        pn.planIndexes[0] = pn.planIndexes[1] = PlanNode.NO_PLAN;
        pn.opCost = pn.totalCost = new Cost(0);
        allPlans.add(pn);

        boolean noCards = false;
        // initialize the level 1 join nodes
        for (int i = 1; i <= numberOfTerms; i++) {
            JoinNode jn = jnArray[i];
            jn.jnArrayIndex = i;
            jn.datasetBits = 1 << (i - 1);
            jn.datasetIndexes = new ArrayList<>(Collections.singleton(i));
            EmptyTupleSourceOperator ets = emptyTupleAndDataSourceOps.get(i - 1).getFirst();
            ILogicalOperator leafInput = joinLeafInputsHashMap.get(ets);

            DataSourceScanOperator scanOp = emptyTupleAndDataSourceOps.get(i - 1).getSecond();
            if (scanOp != null) {
                DataSourceId id = (DataSourceId) scanOp.getDataSource().getId();
                jn.aliases = new ArrayList<>(Collections.singleton(findAlias(scanOp)));
                jn.datasetNames = new ArrayList<>(Collections.singleton(id.getDatasourceName()));
                Index.SampleIndexDetails idxDetails;
                Index index = stats.findSampleIndex(scanOp, optCtx);
                if (index != null) {
                    idxDetails = (Index.SampleIndexDetails) index.getIndexDetails();
                } else {
                    idxDetails = null;
                }

                jn.idxDetails = idxDetails;
                if (cboTestMode) {
                    // to make asterix tests run
                    jn.origCardinality = 1000000;
                    jn.size = 500;
                } else {
                    if (idxDetails == null) {
                        return PlanNode.NO_PLAN;
                    }
                    jn.setOrigCardinality(idxDetails.getSourceCardinality());
                    jn.setAvgDocSize(idxDetails.getSourceAvgItemSize());
                }
                // multiply by the respective predicate selectivities
                jn.cardinality = jn.origCardinality * stats.getSelectivity(leafInput, false);
                if (jn.cardinality < 2.1) {
                    jn.cardinality = 2.1; // for keeping CP and HJ cost formulas happy.
                }
            } else {
                // could be unnest or assign
                jn.datasetNames = new ArrayList<>(Collections.singleton("unnestOrAssign"));
                jn.aliases = new ArrayList<>(Collections.singleton("unnestOrAssign"));
                jn.origCardinality = jn.cardinality = findInListCard(leafInput);
                // just a guess
                jn.size = 10;
            }

            if (jn.origCardinality >= Cost.MAX_CARD) {
                noCards = true;
            }
            jn.correspondingEmptyTupleSourceOp = emptyTupleAndDataSourceOps.get(i - 1).getFirst();
            jn.highestDatasetId = i;
            jn.level = 1;
        }
        if (noCards) {
            return PlanNode.NO_PLAN;
        }
        return numberOfTerms;
    }

    // Most of this work is done in the very first line by calling initializeBaseLevelJoinNodes().
    // the remaining work here is to find the selectivities of the predicates using sampling.
    // By the time execution reaches this point, samples are guaranteed to exist on all datasets,
    // so some of the checks can be removed.
    private int enumerateBaseLevelJoinNodes() throws AlgebricksException {
        int lastBaseLevelJnNum = initializeBaseLevelJoinNodes(); // initialize the level 1 join nodes
        if (lastBaseLevelJnNum == PlanNode.NO_PLAN) {
            return PlanNode.NO_PLAN;
        }

        int dataScanPlan;
        JoinNode[] jnArray = this.getJnArray();
        for (int i = 1; i <= this.numberOfTerms; i++) {
            JoinNode jn = jnArray[i];
            Index.SampleIndexDetails idxDetails = jn.getIdxDetails();
            EmptyTupleSourceOperator ets = this.emptyTupleAndDataSourceOps.get(i - 1).getFirst();
            ILogicalOperator leafInput = this.joinLeafInputsHashMap.get(ets);
            if (!cboTestMode) {
                if (idxDetails == null) {
                    continue;
                }
                double origDatasetCard, finalDatasetCard, sampleCard;

                ILogicalOperator parent = findDataSourceScanOperatorParent(leafInput);
                DataSourceScanOperator scanOp = this.emptyTupleAndDataSourceOps.get(i - 1).getSecond();
                if (scanOp == null) {
                    continue; // what happens to the cards and sizes then? this may happen in case of in lists
                }

                finalDatasetCard = origDatasetCard = idxDetails.getSourceCardinality();

                List<List<IAObject>> result;
                SelectOperator selop = (SelectOperator) findASelectOp(leafInput);

                if (jn.getCardinality() == jn.getOrigCardinality() && selop != null) { // this means there was no selectivity hint provided
                    SampleDataSource sampledatasource = getSampleDataSource(scanOp);
                    DataSourceScanOperator deepCopyofScan =
                            (DataSourceScanOperator) OperatorManipulationUtil.bottomUpCopyOperators(scanOp);
                    deepCopyofScan.setDataSource(sampledatasource);

                    // if there is only one conjunct, I do not have to call the sampling query during index selection!
                    // insert this in place of the scandatasourceOp.
                    parent.getInputs().get(0).setValue(deepCopyofScan);
                    // There are predicates here. So skip the predicates and get the original dataset card.
                    // Now apply all the predicates and get the card after all predicates are applied.
                    result = stats.runSamplingQuery(this.optCtx, leafInput);
                    double predicateCardinality = ((double) ((AInt64) result.get(0).get(0)).getLongValue());
                    if (predicateCardinality == 0.0) {
                        predicateCardinality = 0.0001 * idxDetails.getSampleCardinalityTarget();
                    }
                    // now scale up
                    sampleCard = Math.min(idxDetails.getSampleCardinalityTarget(), origDatasetCard);
                    if (sampleCard == 0) { // should not happen unless the original dataset is empty
                        sampleCard = 1; // we may have to make some adjustments to costs when the sample returns very rows.

                        IWarningCollector warningCollector = optCtx.getWarningCollector();
                        if (warningCollector.shouldWarn()) {
                            warningCollector
                                    .warn(Warning.of(scanOp.getSourceLocation(), ErrorCode.SAMPLE_HAS_ZERO_ROWS));
                        }
                    }

                    finalDatasetCard *= predicateCardinality / sampleCard;
                    // now switch the input back.
                    parent.getInputs().get(0).setValue(scanOp);
                    jn.setCardinality(finalDatasetCard);
                }
            }
            dataScanPlan = jn.addSingleDatasetPlans();
            if (dataScanPlan == PlanNode.NO_PLAN) {
                return PlanNode.NO_PLAN;
            }
            // We may not add any index plans, so need to check for NO_PLAN
            jn.addIndexAccessPlans(leafInput);
        }
        return this.numberOfTerms;
    }

    private boolean isPredicateCardinalityAnnotationPresent(ILogicalExpression leExpr) {
        if (leExpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) leExpr;
            PredicateCardinalityAnnotation pca = afcExpr.getAnnotation(PredicateCardinalityAnnotation.class);
            if (pca != null) {
                return true;
            }
        }
        return false;
    }

    // Since we need to switch the datasource to the sample, we need the parent, so we can do the necessary
    // linked list manipulation.
    protected ILogicalOperator findDataSourceScanOperatorParent(ILogicalOperator op) {
        ILogicalOperator parent = op;
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(LogicalOperatorTag.DATASOURCESCAN)) {
                return parent;
            }
            parent = op;
            op = op.getInputs().get(0).getValue();
        }
        return null;
    }

    // we need to switch the datascource from the dataset source to the corresponding sample datasource.
    // Little tricky how this is done!
    protected SampleDataSource getSampleDataSource(DataSourceScanOperator scanOp) throws AlgebricksException {
        DataverseName dataverseName = stats.findDataverseName(scanOp);
        DataSource ds = (DataSource) scanOp.getDataSource();
        DataSourceId dsid = ds.getId();
        MetadataProvider mdp = (MetadataProvider) this.optCtx.getMetadataProvider();
        Index index = mdp.findSampleIndex(dataverseName, dsid.getDatasourceName());
        DatasetDataSource dds = (DatasetDataSource) ds;
        SampleDataSource sds = new SampleDataSource(dds.getDataset(), index.getIndexName(), ds.getItemType(),
                ds.getMetaItemType(), ds.getDomain());
        return sds;
    }

    private ILogicalOperator findASelectOp(ILogicalOperator op) {

        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {

            if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
                return op;
            }

            op = op.getInputs().get(0).getValue();
        }
        return null;
    }

    // Find the join conditions. Assign selectivities to the join conditions from any user provided annotation hints.
    // If there are no annotation hints, use samples to find the selectivities of the single table predicates
    // found inside of complex join predicates (as in q7). A lot of extra code has gone into making q7 work.
    private void findJoinConditions() throws AlgebricksException {
        findJoinConditionsAndAssignSels();
        List<List<IAObject>> result;
        double predicateCardinality;
        // for all the singleVarExprs, we need to issue a sample query. These exprs did not get assigned a selectivity.
        for (ILogicalExpression exp : this.singleDatasetPreds) {
            if (isPredicateCardinalityAnnotationPresent(exp)) {
                continue; // no need to get selectivity from sample in case of user provided hints.
            }
            List<LogicalVariable> vars = new ArrayList<>();
            exp.getUsedVariables(vars);
            if (vars.size() == 1) { // just being really safe. If samples have size 0, there are issues.
                double origDatasetCard, finalDatasetCard, sampleCard, predicateCard;
                ILogicalOperator leafInput = findLeafInput(vars);
                ILogicalOperator parent = findDataSourceScanOperatorParent(leafInput);
                DataSourceScanOperator scanOp = (DataSourceScanOperator) parent.getInputs().get(0).getValue();

                if (scanOp == null) {
                    continue; // what happens to the cards and sizes then? this may happen in case of in lists
                }

                Index index = getStatsHandle().findSampleIndex(scanOp, this.optCtx);
                if (index == null) {
                    continue; // no sample found
                }
                Index.SampleIndexDetails idxDetails = (Index.SampleIndexDetails) index.getIndexDetails();
                origDatasetCard = idxDetails.getSourceCardinality();
                sampleCard = Math.min(idxDetails.getSampleCardinalityTarget(), origDatasetCard); // handle datasets whose card is small
                if (sampleCard == 0) {
                    sampleCard = 1;
                    IWarningCollector warningCollector = optCtx.getWarningCollector();
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(scanOp.getSourceLocation(), ErrorCode.SAMPLE_HAS_ZERO_ROWS));
                    }
                }

                // replace the dataScanSourceOperator with the sampling source
                SampleDataSource sampledatasource = getSampleDataSource(scanOp);
                DataSourceScanOperator deepCopyofScan =
                        (DataSourceScanOperator) OperatorManipulationUtil.bottomUpCopyOperators(scanOp);
                deepCopyofScan.setDataSource(sampledatasource);

                // insert this in place of the scandatasourceOp.
                parent.getInputs().get(0).setValue(deepCopyofScan);

                // Need to add a selectOperator on top of leafInput.
                SelectOperator selOp = new SelectOperator(new MutableObject<>(exp));
                selOp.getInputs().add(new MutableObject<>(leafInput));
                result = stats.runSamplingQuery(this.optCtx, selOp);
                predicateCardinality = ((double) ((AInt64) result.get(0).get(0)).getLongValue());
                if (predicateCardinality == 0.0) {
                    predicateCardinality = 0.0001 * idxDetails.getSampleCardinalityTarget();
                }

                PredicateCardinalityAnnotation anno =
                        new PredicateCardinalityAnnotation(predicateCardinality / sampleCard);
                AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) exp;
                afce.putAnnotation(anno);
                // now switch the input back.
                parent.getInputs().get(0).setValue(scanOp);
            }
        }

        if (this.singleDatasetPreds.size() > 0) { // We did not have selectivities for these before. Now we do.
            for (JoinCondition jc : joinConditions) {
                jc.selectivity = stats.getSelectivityFromAnnotationMain(jc.getJoinCondition(), false);
                // we may be repeating some work here, but that is ok. This will rarely happen (happens in q7 tpch)
            }
        }
    }

    // main entry point in this file
    protected int enumerateJoins() throws AlgebricksException {
        // create a localJoinOp for use in calling existing nested loops code.
        InnerJoinOperator dummyInput = new InnerJoinOperator(null, null, null);
        localJoinOp = new InnerJoinOperator(new MutableObject<>(ConstantExpression.TRUE),
                new MutableObject<>(dummyInput), new MutableObject<>(dummyInput));

        int lastBaseLevelJnNum = enumerateBaseLevelJoinNodes();
        if (lastBaseLevelJnNum == PlanNode.NO_PLAN) {
            return PlanNode.NO_PLAN;
        }

        IPlanPrettyPrinter pp = optCtx.getPrettyPrinter();
        if (LOGGER.isTraceEnabled()) {
            EnumerateJoinsRule.printPlan(pp, op, "Original Whole plan in JN 1");
        }

        findJoinConditions();
        findIfJoinGraphIsConnected();

        if (LOGGER.isTraceEnabled()) {
            EnumerateJoinsRule.printPlan(pp, op, "Original Whole plan in JN 2");
        }

        markCompositeJoinPredicates();
        int lastJnNum = enumerateHigherLevelJoinNodes();
        JoinNode lastJn = jnArray[lastJnNum];
        if (LOGGER.isTraceEnabled()) {
            EnumerateJoinsRule.printPlan(pp, op, "Original Whole plan in JN END");
            LOGGER.trace(dumpJoinNodes(lastJnNum));
        }

        // find the cheapest plan
        int cheapestPlanIndex = lastJn.cheapestPlanIndex;
        if (LOGGER.isTraceEnabled() && cheapestPlanIndex > 0) {
            LOGGER.trace("Cheapest Plan is {} number of terms is {} joinNodes {}", cheapestPlanIndex, numberOfTerms,
                    lastJnNum);
        }

        return cheapestPlanIndex;
    }

    private String dumpJoinNodes(int numJoinNodes) {
        StringBuilder sb = new StringBuilder(128);
        sb.append(LocalDateTime.now());
        for (int i = 1; i <= numJoinNodes; i++) {
            JoinNode jn = jnArray[i];
            sb.append(jn);
        }
        sb.append('\n').append("Printing cost of all Final Plans").append('\n');
        jnArray[numJoinNodes].printCostOfAllPlans(sb);
        return sb.toString();
    }

    private static boolean getForceJoinOrderMode(IOptimizationContext context) {
        PhysicalOptimizationConfig physOptConfig = context.getPhysicalOptimizationConfig();
        return physOptConfig.getForceJoinOrderMode();
    }

    private static String getQueryPlanShape(IOptimizationContext context) {
        PhysicalOptimizationConfig physOptConfig = context.getPhysicalOptimizationConfig();
        return physOptConfig.getQueryPlanShapeMode();
    }
}
