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
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.declared.SampleDataSource;
import org.apache.asterix.metadata.entities.Index;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.PredicateCardinalityAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.control.common.config.OptionTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JoinEnum {

    private static final Logger LOGGER = LogManager.getLogger();

    // Number of levels to do full join and plan enumeration
    public static final String CBO_FULL_ENUM_LEVEL_KEY = "cbofullenumlevel";
    private static final int CBO_FULL_ENUM_LEVEL_DEFAULT = 0;

    // Mode for cartesian product plan generation during join and plan enumeration
    public static final String CBO_CP_ENUM_KEY = "cbocpenum";
    private static final boolean CBO_CP_ENUM_DEFAULT = true;
    protected List<JoinCondition> joinConditions; // "global" list of join conditions
    protected Map<IExpressionAnnotation, Warning> joinHints;
    protected List<PlanNode> allPlans; // list of all plans
    protected JoinNode[] jnArray; // array of all join nodes
    protected int jnArraySize;
    protected List<ILogicalOperator> leafInputs;

    // The Distinct operators for each DataScan operator (if applicable)
    protected HashMap<DataSourceScanOperator, ILogicalOperator> dataScanAndGroupByDistinctOps;

    // The Distinct/GroupBy operator at root of the query tree (if exists)
    protected ILogicalOperator rootGroupByDistinctOp;

    // The OrderBy operator at root of the query tree (if exists)
    protected ILogicalOperator rootOrderByOp;
    protected List<ILogicalExpression> singleDatasetPreds;
    protected List<AssignOperator> assignOps;
    List<Quadruple<Integer, Integer, JoinOperator, Integer>> outerJoinsDependencyList;
    HashMap<LogicalVariable, Integer> varLeafInputIds;
    protected List<JoinOperator> allJoinOps;
    protected ILogicalOperator localJoinOp; // used in nestedLoopsApplicable code.
    protected IOptimizationContext optCtx;
    protected boolean outerJoin;
    protected List<Triple<Integer, Integer, Boolean>> buildSets;
    protected int allTabsJnNum; // keeps track of the join Node where all the tables have been joined
    protected int maxBits; // the joinNode where the dataset bits are the highest is where all the tables have been joined

    protected Stats stats;
    private boolean cboMode;
    private boolean cboTestMode;
    protected int cboFullEnumLevel;
    protected boolean cboCPEnumMode;
    protected int numberOfTerms;
    private AbstractLogicalOperator op;
    protected boolean connectedJoinGraph;
    protected boolean forceJoinOrderMode;
    protected String queryPlanShape;
    protected ICost cost;
    protected ICostMethods costMethods;
    List<LogicalVariable> resultAndJoinVars;

    public JoinEnum() {
    }

    protected void initEnum(AbstractLogicalOperator op, boolean cboMode, boolean cboTestMode, int numberOfFromTerms,
            List<ILogicalOperator> leafInputs, List<JoinOperator> allJoinOps, List<AssignOperator> assignOps,
            List<Quadruple<Integer, Integer, JoinOperator, Integer>> outerJoinsDependencyList,
            List<Triple<Integer, Integer, Boolean>> buildSets, HashMap<LogicalVariable, Integer> varLeafInputIds,
            HashMap<DataSourceScanOperator, ILogicalOperator> dataScanAndGroupByDistinctOps,
            ILogicalOperator grpByDistinctOp, ILogicalOperator orderByOp, List<LogicalVariable> resultAndJoinVars,
            IOptimizationContext context) throws AsterixException {
        this.singleDatasetPreds = new ArrayList<>();
        this.joinConditions = new ArrayList<>();
        this.joinHints = new HashMap<>();
        this.allPlans = new ArrayList<>();
        this.numberOfTerms = numberOfFromTerms;
        this.cboMode = cboMode;
        this.cboTestMode = cboTestMode;
        this.cboFullEnumLevel = getCBOFullEnumLevel(context);
        this.cboCPEnumMode = getCBOCPEnumMode(context);
        this.connectedJoinGraph = true;
        this.optCtx = context;
        this.leafInputs = leafInputs;
        this.assignOps = assignOps;
        this.outerJoin = false; // assume no outerjoins anywhere in the query at first.
        this.outerJoinsDependencyList = outerJoinsDependencyList;
        this.allJoinOps = allJoinOps;
        this.buildSets = buildSets;
        this.varLeafInputIds = varLeafInputIds;
        this.dataScanAndGroupByDistinctOps = dataScanAndGroupByDistinctOps;
        this.rootGroupByDistinctOp = grpByDistinctOp;
        this.rootOrderByOp = orderByOp;
        this.resultAndJoinVars = resultAndJoinVars;
        this.op = op;
        this.forceJoinOrderMode = getForceJoinOrderMode(context);
        this.queryPlanShape = getQueryPlanShape(context);
        initCostHandleAndJoinNodes(context);
        this.allTabsJnNum = 1; // keeps track of where the final join Node will be. In case of bushy plans, this may not always be the last join nod     e.
        this.maxBits = 1;
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

    private int getCBOFullEnumLevel(IOptimizationContext context) throws AsterixException {
        MetadataProvider mdp = (MetadataProvider) context.getMetadataProvider();

        String valueInQuery = mdp.getProperty(CBO_FULL_ENUM_LEVEL_KEY);
        try {
            return valueInQuery == null ? CBO_FULL_ENUM_LEVEL_DEFAULT
                    : OptionTypes.POSITIVE_INTEGER.parse(valueInQuery);
        } catch (IllegalArgumentException e) {
            throw AsterixException.create(ErrorCode.COMPILATION_BAD_QUERY_PARAMETER_VALUE, CBO_FULL_ENUM_LEVEL_KEY, 1,
                    "");
        }
    }

    private boolean getCBOCPEnumMode(IOptimizationContext context) {
        MetadataProvider mdp = (MetadataProvider) context.getMetadataProvider();
        return mdp.getBooleanProperty(CBO_CP_ENUM_KEY, CBO_CP_ENUM_DEFAULT);
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
        for (ILogicalOperator op : leafInputs) {
            vars.clear();
            // this is expensive to do. So store this once and reuse
            VariableUtilities.getLiveVariables(op, vars);
            if (vars.containsAll(logicalVars)) {
                return op;
            }
        }

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

    protected boolean lookForOuterJoins(List<Integer> newJoinConditions) {
        for (int joinNum : newJoinConditions) {
            JoinCondition jc = joinConditions.get(joinNum);
            if (jc.outerJoin) {
                return true;
            }
        }
        return false;
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
        return JoinNode.NO_JN;
    }

    // This finds all the join Conditions in the whole query. This is a global list of all join predicates.
    // It also fills in the dataset Bits for each join predicate.
    private void findJoinConditionsAndAssignSels() throws AlgebricksException {

        List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
        for (JoinOperator jOp : allJoinOps) {
            AbstractBinaryJoinOperator joinOp = jOp.getAbstractJoinOp();
            ILogicalExpression expr = joinOp.getCondition().getValue();
            conjs.clear();
            if (expr.splitIntoConjuncts(conjs)) {
                conjs.remove(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
                for (Mutable<ILogicalExpression> conj : conjs) {
                    JoinCondition jc = new JoinCondition();
                    jc.outerJoin = jOp.getOuterJoin();
                    if (jc.outerJoin) {
                        outerJoin = true;
                    }
                    jc.joinCondition = conj.getValue().cloneExpression();
                    joinConditions.add(jc);
                }
            } else {
                if ((expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL)) {
                    JoinCondition jc = new JoinCondition();
                    jc.outerJoin = jOp.getOuterJoin();
                    if (jc.outerJoin) {
                        outerJoin = true;
                    }
                    // change to not a true condition
                    jc.joinCondition = expr.cloneExpression();
                    joinConditions.add(jc);
                }
            }
        }

        // now patch up any join conditions that have variables referenced in any internal assign statements.
        List<LogicalVariable> usedVars = new ArrayList<>();
        List<AssignOperator> erase = new ArrayList<>();
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
                        erase.add(aOp);
                    }
                }
            }
            jc.selectivity = stats.getSelectivityFromAnnotationMain(jc.joinCondition, true, false);
        }
        for (int i = erase.size() - 1; i >= 0; i--) {
            assignOps.remove(erase.get(i));
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
                int bits = 1 << (varLeafInputIds.get(usedVars.get(i)) - 1);
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

    private boolean isThisCombinationPossible(JoinNode leftJn, JoinNode rightJn) {
        for (Quadruple<Integer, Integer, JoinOperator, Integer> tr : outerJoinsDependencyList) {
            if (tr.getThird().getOuterJoin()) {
                if (rightJn.datasetBits == tr.getSecond()) { // A dependent table(s) is being joined. Find if other table(s) is present
                    if (!((leftJn.datasetBits & tr.getFirst()) > 0)) {
                        return false; // required table not found
                    }
                }
            }
        }

        if (leftJn.level == 1) { // if we are at a higher level, there is nothing to check as these tables have been joined already in leftJn
            for (Quadruple<Integer, Integer, JoinOperator, Integer> tr : outerJoinsDependencyList) {
                if (tr.getThird().getOuterJoin()) {
                    if (leftJn.datasetBits == tr.getSecond()) { // A dependent table(s) is being joined. Find if other table(s) is present
                        if (!((rightJn.datasetBits & tr.getFirst()) > 0)) {
                            return false; // required table not found
                        }
                    }
                }
            }
        }
        return true;
    }

    private int findBuildSet(int jbits, int numbTabs) {
        int i;
        if (buildSets.isEmpty()) {
            return -1;
        }
        for (i = 0; i < buildSets.size(); i++) {
            if ((buildSets.get(i).third) && (buildSets.get(i).first & jbits) > 0) {
                return i;
            }
        }
        return -1;
    }

    private int addNonBushyJoinNodes(int level, int jnNumber, int[] startJnAtLevel) throws AlgebricksException {
        // adding joinNodes of level (2, 3, ..., numberOfTerms)
        int startJnSecondLevel = startJnAtLevel[2];
        int startJnPrevLevel = startJnAtLevel[level - 1];
        int startJnNextLevel = startJnAtLevel[level];
        int i, j, k, addPlansToThisJn;

        // walking thru the previous level
        for (i = startJnPrevLevel; i < startJnNextLevel; i++) {
            JoinNode jnI = jnArray[i];
            jnI.jnArrayIndex = i;
            if (jnI.highestDatasetId == 0) {
                continue;
            }

            int endLevel;
            if (outerJoin && buildSets.size() > 0) { // we do not need outerJoin here but ok for now. BuildSets are built only when we have outerjoins
                endLevel = startJnNextLevel; // bushy trees possible
            } else {
                endLevel = startJnSecondLevel; // no bushy trees
            }

            for (j = 1; j < endLevel; j++) { // this enables bushy plans; dangerous :-) should be done only if outer joins are present.
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
                //System.out.println("Before1 i = " + i + " j = " + j); // will put these in trace statements soon
                //System.out.println("Before1 Jni Dataset bits = " + jnI.datasetBits + " Jni Dataset bits = " + jnJ.datasetBits);
                // first check if the new table is part of a buildSet.
                k = findBuildSet(jnJ.datasetBits, jnI.level + jnJ.level);
                //System.out.println("Buildset " + k);
                if (k > -1) {
                    if ((jnI.datasetBits & buildSets.get(k).first) == 0) { // i should also be part of the buildSet
                        continue;
                    }
                }
                //System.out.println("Before2 i = " + i + " j = " + j); // put these in trace statements
                //System.out.println("Before2 Jni Dataset bits = " + jnI.datasetBits + " Jni Dataset bits = " + jnJ.datasetBits);
                //System.out.println("Before i = " + i + " j = " + j);
                if (!isThisCombinationPossible(jnI, jnJ)) {
                    continue;
                }
                //System.out.println("After i = " + i + " j = " + j); //put these in trace statements
                //System.out.println("After Jni Dataset bits = " + jnI.datasetBits + " Jni Dataset bits = " + jnJ.datasetBits);
                int newBits = jnI.datasetBits | jnJ.datasetBits;
                if ((k > 0) && (newBits == buildSets.get(k).first)) { // This buildSet is no longer needed.
                    buildSets.get(k).third = false;
                }
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
                    if (newBits > maxBits) {
                        maxBits = newBits;
                        allTabsJnNum = jnNumber;
                    }
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
                    jn.size = jnI.size + jnJ.size; // These are the original document sizes
                    jn.setCardinality(jn.computeJoinCardinality(), true);
                    jn.setSizeVarsAfterScan(jnI.getSizeVarsAfterScan() + jnJ.getSizeVarsAfterScan());
                } else {
                    addPlansToThisJn = jnNewBits.jnIndex;
                }

                JoinNode jnIJ = jnArray[addPlansToThisJn];
                jnIJ.jnArrayIndex = addPlansToThisJn;

                jnIJ.addMultiDatasetPlans(jnI, jnJ);
                if (forceJoinOrderMode && level > cboFullEnumLevel) {
                    break;
                }
            }
            if (forceJoinOrderMode && level > cboFullEnumLevel) {
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

        double grpInputCard = (double) Math.round(jnArray[jnNumber].getCardinality() * 100) / 100;
        double grpOutputCard =
                (double) Math.round(Math.min(grpInputCard, jnArray[jnNumber].distinctCardinality) * 100) / 100;

        // set the root group-by/distinct operator's cardinality annotations (if exists)
        if (!cboTestMode && this.rootGroupByDistinctOp != null) {
            this.rootGroupByDistinctOp.getAnnotations().put(OperatorAnnotations.OP_INPUT_CARDINALITY, grpInputCard);
            this.rootGroupByDistinctOp.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_CARDINALITY, grpOutputCard);
        }

        // set the root order by operator's cardinality annotations (if exists)
        if (!cboTestMode && this.rootOrderByOp != null) {
            if (this.rootGroupByDistinctOp != null) {
                this.rootOrderByOp.getAnnotations().put(OperatorAnnotations.OP_INPUT_CARDINALITY, grpOutputCard);
                this.rootOrderByOp.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_CARDINALITY, grpOutputCard);
            } else {
                this.rootOrderByOp.getAnnotations().put(OperatorAnnotations.OP_INPUT_CARDINALITY, grpInputCard);
                this.rootOrderByOp.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_CARDINALITY, grpInputCard);
            }
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
            ILogicalOperator leafInput = leafInputs.get(i - 1);
            DataSourceScanOperator scanOp = findDataSourceScanOperator(leafInput);
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
                    jn.setOrigCardinality(idxDetails.getSourceCardinality(), false);
                    jn.setAvgDocSize(idxDetails.getSourceAvgItemSize());
                    jn.setSizeVarsFromDisk(10); // dummy value
                    jn.setSizeVarsAfterScan(10); // dummy value
                }
                // multiply by the respective predicate selectivities
                jn.setCardinality(jn.origCardinality * stats.getSelectivity(leafInput, false), false);
            } else {
                // could be unnest or assign
                jn.datasetNames = new ArrayList<>(Collections.singleton("unnestOrAssign"));
                jn.aliases = new ArrayList<>(Collections.singleton("unnestOrAssign"));
                double card = findInListCard(leafInput);
                jn.setOrigCardinality(card, false);
                jn.setCardinality(card, false);
                // just a guess
                jn.size = 10;
            }

            if (jn.origCardinality >= Cost.MAX_CARD) {
                noCards = true;
            }
            jn.leafInput = leafInputs.get(i - 1);
            jn.highestDatasetId = i;
            jn.level = 1;
        }
        if (noCards) {
            return PlanNode.NO_PLAN;
        }
        return numberOfTerms;
    }

    protected DataSourceScanOperator findDataSourceScanOperator(ILogicalOperator op) {
        ILogicalOperator origOp = op;
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(LogicalOperatorTag.DATASOURCESCAN)) {
                return (DataSourceScanOperator) op;
            }
            op = op.getInputs().get(0).getValue();
        }
        return null;
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
        int limit = -1;
        if (this.numberOfTerms == 1) {
            jnArray[1].setLimitVal(findLimitValue(this.op));
        }
        for (int i = 1; i <= this.numberOfTerms; i++) {
            JoinNode jn = jnArray[i];
            Index.SampleIndexDetails idxDetails = jn.getIdxDetails();
            ILogicalOperator leafInput = this.leafInputs.get(i - 1);
            if (!cboTestMode) {
                if (idxDetails == null) {
                    dataScanPlan = jn.addSingleDatasetPlans();
                    if (dataScanPlan == PlanNode.NO_PLAN) {
                        return PlanNode.NO_PLAN;
                    }
                    continue;
                }
                jn.setCardsAndSizes(idxDetails, leafInput);

                // Compute the distinct cardinalities for each base join node.
                DataSourceScanOperator scanOp = findDataSourceScanOperator(leafInput);
                ILogicalOperator grpByDistinctOp = this.dataScanAndGroupByDistinctOps.get(scanOp);
                if (grpByDistinctOp != null) {
                    long distinctCardinality = stats.findDistinctCardinality(grpByDistinctOp);
                    jn.distinctCardinality = (double) distinctCardinality;
                    double grpInputCard = (double) Math.round(jn.cardinality * 100) / 100;
                    double grpOutputCard = (double) Math.round(Math.min(grpInputCard, distinctCardinality) * 100) / 100;
                    grpByDistinctOp.getAnnotations().put(OperatorAnnotations.OP_INPUT_CARDINALITY, grpInputCard);
                    grpByDistinctOp.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_CARDINALITY, grpOutputCard);
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

    private int findLimitValue(AbstractLogicalOperator oper) {
        ILogicalOperator op = oper;
        int limit = -1;
        while (op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag() == LogicalOperatorTag.LIMIT) {
                LimitOperator lop = (LimitOperator) op;
                ILogicalExpression expr = lop.getMaxObjects().getValue();
                if (expr != null) {
                    if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) { // must be a constant
                        limit = Integer.parseInt(lop.getMaxObjects().getValue().toString());
                    }
                }
            } else if (op.getOperatorTag() == LogicalOperatorTag.ORDER) {
                return -1; // This is because we cant reduce the selectivity of a scan operator when an order by is present.
            } else if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
                return -1; // This is because we cant reduce the selectivity of a scan operator when a group by is present.
            }
            op = op.getInputs().get(0).getValue();
        }
        return limit;
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
        DataSource ds = (DataSource) scanOp.getDataSource();
        DataSourceId dsid = ds.getId();
        MetadataProvider mdp = (MetadataProvider) this.optCtx.getMetadataProvider();
        Index index = mdp.findSampleIndex(dsid.getDatabaseName(), dsid.getDataverseName(), dsid.getDatasourceName());
        DatasetDataSource dds = (DatasetDataSource) ds;
        return new SampleDataSource(dds.getDataset(), index.getIndexName(), ds.getItemType(), ds.getMetaItemType(),
                ds.getDomain());
    }

    protected ILogicalOperator findASelectOp(ILogicalOperator op) {
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {

            if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
                return op;
            }
            op = op.getInputs().get(0).getValue();
        }
        return null;
    }

    private boolean findUnnestOp(ILogicalOperator op) {
        ILogicalOperator currentOp = op;
        while (currentOp != null && currentOp.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (currentOp.getOperatorTag().equals(LogicalOperatorTag.UNNEST)) {
                return true;
            }
            currentOp = currentOp.getInputs().get(0).getValue();
        }
        return false;
    }

    // Find the join conditions. Assign selectivities to the join conditions from any user provided annotation hints.
    // If there are no annotation hints, use samples to find the selectivities of the single table predicates
    // found inside of complex join predicates (as in q7). A lot of extra code has gone into making q7 work.
    private void findJoinConditions() throws AlgebricksException {
        findJoinConditionsAndAssignSels();
        // for all the singleVarExprs, we need to issue a sample query. These exprs did not get assigned a selectivity.
        for (ILogicalExpression exp : this.singleDatasetPreds) {
            if (isPredicateCardinalityAnnotationPresent(exp)) {
                continue; // no need to get selectivity from sample in case of user provided hints.
            }
            List<LogicalVariable> vars = new ArrayList<>();
            exp.getUsedVariables(vars);
            if (vars.size() == 1) { // just being really safe. If samples have size 0, there are issues.
                double sel;
                ILogicalOperator leafInput = findLeafInput(vars);
                SelectOperator selOp;
                if (leafInput.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                    selOp = (SelectOperator) leafInput;
                } else {
                    selOp = new SelectOperator(new MutableObject<>(exp));
                    selOp.getInputs().add(new MutableObject<>(leafInput));
                }
                sel = getStatsHandle().findSelectivityForThisPredicate(selOp, (AbstractFunctionCallExpression) exp,
                        findUnnestOp(leafInput));
                // Sometimes the sample query returns greater more rows than the sample size. Cap the selectivity to 0.9999
                sel = Math.min(sel, 0.9999);

                // Add the selectivity annotation.
                PredicateCardinalityAnnotation anno = new PredicateCardinalityAnnotation(sel);
                AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) exp;
                afce.putAnnotation(anno);
            }
        }

        if (this.singleDatasetPreds.size() > 0) { // We did not have selectivities for these before. Now we do.
            for (JoinCondition jc : joinConditions) {
                // we may be repeating some work here, but that is ok. This will rarely happen (happens in q7 tpch)
                double sel = stats.getSelectivityFromAnnotationMain(jc.getJoinCondition(), false, true);
                if (sel != -1) {
                    jc.selectivity = sel;
                }
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
        JoinNode lastJn = jnArray[allTabsJnNum];
        if (LOGGER.isTraceEnabled()) {
            EnumerateJoinsRule.printPlan(pp, op, "Original Whole plan in JN END");
            LOGGER.trace(dumpJoinNodes(lastJnNum));
        }

        // return the cheapest plan
        return lastJn.cheapestPlanIndex;
    }

    private String dumpJoinNodes(int numJoinNodes) {
        StringBuilder sb = new StringBuilder(128);
        sb.append(LocalDateTime.now());
        dumpContext(sb);
        for (int i = 1; i <= numJoinNodes; i++) {
            JoinNode jn = jnArray[i];
            sb.append(jn);
        }
        sb.append("Number of terms is ").append(numberOfTerms).append(", Number of Join Nodes is ").append(numJoinNodes)
                .append('\n');
        sb.append("Printing cost of all Final Plans").append('\n');
        jnArray[numJoinNodes].printCostOfAllPlans(sb);
        return sb.toString();
    }

    private void dumpContext(StringBuilder sb) {
        sb.append("\n\nOPT CONTEXT").append('\n');
        sb.append("----------------------------------------\n");
        sb.append("BLOCK SIZE = ").append(getCostMethodsHandle().getBufferCachePageSize()).append('\n');
        sb.append("DOP = ").append(getCostMethodsHandle().getDOP()).append('\n');
        sb.append("MAX MEMORY SIZE FOR JOIN = ").append(getCostMethodsHandle().getMaxMemorySizeForJoin()).append('\n');
        sb.append("MAX MEMORY SIZE FOR GROUP = ").append(getCostMethodsHandle().getMaxMemorySizeForGroup())
                .append('\n');
        sb.append("MAX MEMORY SIZE FOR SORT = ").append(getCostMethodsHandle().getMaxMemorySizeForSort()).append('\n');
        sb.append("----------------------------------------\n");
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