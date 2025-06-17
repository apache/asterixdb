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

import static org.apache.asterix.om.functions.BuiltinFunctions.getBuiltinFunctionInfo;

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
import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.util.FunctionUtil;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
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
    protected List<List<List<ILogicalOperator>>> unnestOpsInfo;
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
    Map<DataSourceScanOperator, Boolean> fakeLeafInputsMap;

    public JoinEnum() {
    }

    protected void initEnum(AbstractLogicalOperator op, boolean cboMode, boolean cboTestMode, int numberOfFromTerms,
            List<ILogicalOperator> leafInputs, List<JoinOperator> allJoinOps, List<AssignOperator> assignOps,
            List<Quadruple<Integer, Integer, JoinOperator, Integer>> outerJoinsDependencyList,
            List<Triple<Integer, Integer, Boolean>> buildSets, HashMap<LogicalVariable, Integer> varLeafInputIds,
            List<List<List<ILogicalOperator>>> unnestOpsInfo,
            HashMap<DataSourceScanOperator, ILogicalOperator> dataScanAndGroupByDistinctOps,
            ILogicalOperator grpByDistinctOp, ILogicalOperator orderByOp, List<LogicalVariable> resultAndJoinVars,
            Map<DataSourceScanOperator, Boolean> fakeLeafInputsMap, IOptimizationContext context)
            throws AsterixException {
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
        this.unnestOpsInfo = unnestOpsInfo;
        this.dataScanAndGroupByDistinctOps = dataScanAndGroupByDistinctOps;
        this.rootGroupByDistinctOp = grpByDistinctOp;
        this.rootOrderByOp = orderByOp;
        this.resultAndJoinVars = resultAndJoinVars;
        this.fakeLeafInputsMap = fakeLeafInputsMap;
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
        ScalarFunctionCallExpression andExpr =
                new ScalarFunctionCallExpression(getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));

        for (int joinNum : newJoinConditions) {
            // Need to AND all the expressions.
            JoinCondition jc = joinConditions.get(joinNum);
            andExpr.getArguments().add(new MutableObject<>(jc.joinCondition));
        }
        return andExpr;
    }

    protected ILogicalExpression getNestedLoopJoinExpr(List<Integer> newJoinConditions) {
        if (newJoinConditions.size() == 0) {
            // this is a cartesian product
            return ConstantExpression.TRUE;
        }
        if (newJoinConditions.size() == 1) {
            JoinCondition jc = joinConditions.get(newJoinConditions.get(0));
            return jc.joinCondition;
        }
        ScalarFunctionCallExpression andExpr =
                new ScalarFunctionCallExpression(getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));
        for (int joinNum : newJoinConditions) {
            // need to AND all the expressions. skip derived exprs for now.
            JoinCondition jc = joinConditions.get(joinNum);
            if (jc.derived) {
                continue;
            }
            andExpr.getArguments().add(new MutableObject<>(jc.joinCondition));
        }

        if (andExpr.getArguments().size() == 1) {
            return andExpr.getArguments().get(0).getValue(); // remove the AND if there is only one argument
        } else if (andExpr.getArguments().size() > 1) {
            return null; // the nested loops code expects only one predicate of the type R.a op S.a
        }
        return andExpr;
    }

    protected ILogicalExpression getHashJoinExpr(List<Integer> newJoinConditions, boolean outerJoin) {
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
        ScalarFunctionCallExpression andExpr =
                new ScalarFunctionCallExpression(getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));

        // at least one equality predicate needs to be present for a hash join to be possible.
        boolean eqPredFound = false;
        for (int joinNum : newJoinConditions) {
            // need to AND all the expressions.
            JoinCondition jc = joinConditions.get(joinNum);
            if (jc.comparisonType == JoinCondition.comparisonOp.OP_EQ) {
                eqPredFound = true;
            } else if (outerJoin) {
                // For outer joins, non-eq predicates cannot be pulled up and applied after the
                // join, so a hash join will not be possible.
                return null;
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
        if (condition == null) {
            return false;
        }
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

    public SkipSecondaryIndexSearchExpressionAnnotation findSkipIndexHint(AbstractFunctionCallExpression condition) {
        if (condition.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND)) {
            for (int i = 0; i < condition.getArguments().size(); i++) {
                ILogicalExpression expr = condition.getArguments().get(i).getValue();
                if (expr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                    AbstractFunctionCallExpression AFCexpr = (AbstractFunctionCallExpression) expr;
                    SkipSecondaryIndexSearchExpressionAnnotation skipAnno =
                            AFCexpr.getAnnotation(SkipSecondaryIndexSearchExpressionAnnotation.class);
                    if (skipAnno != null) {
                        return skipAnno;
                    }
                }
            }
        } else if (condition.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            SkipSecondaryIndexSearchExpressionAnnotation skipAnno =
                    condition.getAnnotation(SkipSecondaryIndexSearchExpressionAnnotation.class);
            if (skipAnno != null) {
                return skipAnno;
            }
        }
        return null;
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
    // It also fills in the dataset Bits for each join predicate. Add Transitive Join Predicates also.
    private void findJoinConditionsAndDoTC() throws AlgebricksException {
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
                    jc.joinCondition = conj.getValue();
                    LOGGER.info("adding JC " + jc.joinCondition);
                    jc.usedVars = getUsedVars(jc);
                    joinConditions.add(jc);
                    jc.joinOp = jOp;
                }
            } else {
                if ((expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL)) {
                    JoinCondition jc = new JoinCondition();
                    jc.outerJoin = jOp.getOuterJoin();
                    if (jc.outerJoin) {
                        outerJoin = true;
                    }
                    jc.joinCondition = expr;
                    LOGGER.info("adding JC " + jc.joinCondition);
                    jc.usedVars = getUsedVars(jc);
                    joinConditions.add(jc);
                    jc.joinOp = jOp;
                }
            }
        }
        addTCJoinPreds(); // transitive close of join predicates
        // now patch up any join conditions that have variables referenced in any internal assign statements.
        List<LogicalVariable> usedVars = new ArrayList<>();
        List<AssignOperator> erase = new ArrayList<>();
        for (JoinCondition jc : joinConditions) {
            ILogicalExpression expr = jc.joinCondition;
            AbstractFunctionCallExpression aexpr = (AbstractFunctionCallExpression) expr;
            usedVars.clear();
            expr.getUsedVariables(usedVars);
            boolean fixed = false;
            for (AssignOperator aOp : assignOps) { // These assignOps are internal assignOps (found between join nodes)
                for (int i = 0; i < aOp.getVariables().size(); i++) {
                    if (usedVars.contains(aOp.getVariables().get(i))) {
                        OperatorManipulationUtil.replaceVarWithExpr((AbstractFunctionCallExpression) expr,
                                aOp.getVariables().get(i), aOp.getExpressions().get(i).getValue());
                        jc.joinCondition = expr;
                        erase.add(aOp);
                        fixed = true;
                    }
                }
            }
            if (!fixed) {
                // now comes the hard part. Need to look thru all the assigns in the leafInputs
                for (ILogicalOperator op : leafInputs) {
                    while (op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
                        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                            AssignOperator aOp = (AssignOperator) op;
                            ILogicalExpression a = aOp.getExpressions().get(0).getValue();
                            usedVars.clear();
                            a.getUsedVariables(usedVars);
                            if (usedVars.size() > 1) {
                                for (int i = 0; i < aOp.getVariables().size(); i++) {
                                    if (usedVars.contains(aOp.getVariables().get(i))) {
                                        OperatorManipulationUtil.replaceVarWithExpr(
                                                (AbstractFunctionCallExpression) expr, aOp.getVariables().get(i),
                                                aOp.getExpressions().get(i).getValue());
                                        jc.joinCondition = expr;
                                    }
                                }
                            }
                        }
                        op = op.getInputs().get(0).getValue();
                    }
                }
            }
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

            List<Integer> leafInputNumbers = new ArrayList<>(jc.numberOfVars);
            for (int i = 0; i < jc.numberOfVars; i++) {
                int idx = varLeafInputIds.get(usedVars.get(i));
                if (!leafInputNumbers.contains(idx)) {
                    leafInputNumbers.add(idx);
                }
            }
            jc.numLeafInputs = leafInputNumbers.size();
            for (int i = 0; i < jc.numLeafInputs; i++) {
                int side = leafInputNumbers.get(i);
                int bits = 1 << (side - 1);
                if (i == 0) {
                    jc.leftSide = side;
                    jc.leftSideBits = bits;
                } else if (i == 1) {
                    jc.rightSide = side;
                    jc.rightSideBits = bits;
                }
                jc.datasetBits |= bits;
            }
        }
    }

    // transitive close of join predicates; add only if they are not already present; user may have added them in the query
    private void addTCJoinPreds() {
        boolean changes = true;
        while (changes) {
            changes = false;
            int size = joinConditions.size(); // store the size here. We will add more join conditions.
            for (int i = 0; i < size - 1; i++) {
                List<LogicalVariable> vars1 = joinConditions.get(i).usedVars; // see if the predicate just added will yield any TC preds.
                if (vars1 != null) {
                    for (int j = i + 1; j < size; j++) {
                        ILogicalExpression newExpr = null;
                        List<LogicalVariable> vars2 = joinConditions.get(j).usedVars;
                        if (vars2 != null) {
                            if (vars1.get(0) == vars2.get(0)) {
                                if (notFound(vars1.get(1), vars2.get(1))) {
                                    newExpr = makeNewEQJoinExpr(vars1.get(1), vars2.get(1));
                                }
                            } else if (vars1.get(0) == vars2.get(1)) {
                                if (notFound(vars1.get(1), vars2.get(0))) {
                                    newExpr = makeNewEQJoinExpr(vars1.get(1), vars2.get(0));
                                }
                            } else if (vars1.get(1) == vars2.get(1)) {
                                if (notFound(vars1.get(0), vars2.get(0))) {
                                    newExpr = makeNewEQJoinExpr(vars1.get(0), vars2.get(0));
                                }
                            } else if (vars1.get(1) == vars2.get(0)) {
                                if (notFound(vars1.get(0), vars2.get(1))) {
                                    newExpr = makeNewEQJoinExpr(vars1.get(0), vars2.get(1));
                                }
                            }
                        }
                        if (newExpr != null) {
                            changes = true;
                            LOGGER.info("vars1 " + vars1 + "; vars2 " + vars2 + " = " + "newExpr " + newExpr);
                            JoinCondition jc = new JoinCondition();
                            jc.outerJoin = false;
                            jc.derived = true; // useful to exclude for NL Joins since NL joins can take only one pred
                            jc.joinCondition = newExpr;
                            jc.usedVars = getUsedVars(jc);
                            joinConditions.add(jc);
                            jc.joinOp = joinConditions.get(i).joinOp; // borrowing the joinOp here as this does not have a joinOp of its own.
                        }
                    }
                }
            }
        }
    }

    protected ILogicalExpression makeNewEQJoinExpr(LogicalVariable var1, LogicalVariable var2) {
        if (varLeafInputIds.get(var1) == varLeafInputIds.get(var2)) {
            return null; // must be from different datasets to make a join expression
        }
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<>();
        VariableReferenceExpression e1 = new VariableReferenceExpression(var1);
        arguments.add(new MutableObject<>(e1));
        VariableReferenceExpression e2 = new VariableReferenceExpression(var2);
        arguments.add(new MutableObject<>(e2));
        ScalarFunctionCallExpression expr = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.EQ), arguments);
        return expr;
    }

    private boolean notFound(LogicalVariable var1, LogicalVariable var2) {
        for (int i = 0; i < joinConditions.size(); i++) {
            List<LogicalVariable> vars = joinConditions.get(i).usedVars;
            if (vars != null) {
                if (vars.get(0) == var1 && vars.get(1) == var2) {
                    return false;
                }
                if (vars.get(1) == var1 && vars.get(0) == var2) {
                    return false;
                }
            }
        }
        return true;
    }

    // This routine is collecting information about the variables in the predicates to see if we can do a Transitive closure.
    // Only considering equi join predicates for now.
    private List<LogicalVariable> getUsedVars(JoinCondition jc) {
        ILogicalExpression exp = jc.joinCondition;
        if (!jc.outerJoin) {
            if (exp.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                AbstractFunctionCallExpression afcexpr = (AbstractFunctionCallExpression) exp;
                if (afcexpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.EQ)) {
                    List<LogicalVariable> usedVars = new ArrayList<>();
                    exp.getUsedVariables(usedVars);
                    if (usedVars.size() == 2) {
                        return usedVars;
                    }
                }
            }
        }
        return null;
    }

    // This is basically a heuristic routine. Not guaranteed to be 100% accurate.
    // Use only when primary keys are not defined (shadow data sets)
    // Examples l.partkey = ps.partkey and l.suppkey = ps.suppkey TPCH
    // c.c_d_id = o.o_d_id and c.c_w_id = o.o_w_id and c.c_id = o.o_c_id in CH2
    //for (JoinCondition jc : joinConditions) {
    //jc.selectivity = stats.getSelectivityFromAnnotationMain(jc, null, true, false, jc.joinOp);
    //}
    private void markCompositeJoinPredicates() throws AlgebricksException {
        List<JoinCondition> JCs = new ArrayList<>();
        for (int i = 0; i < joinConditions.size(); i++) {
            JoinCondition jcI = joinConditions.get(i);
            if (jcI.usedVars == null || jcI.usedVars.size() != 2 || jcI.numLeafInputs != 2 || jcI.partOfComposite) {
                continue;
            }
            JCs.clear();
            JCs.add(jcI);
            if (jcI.comparisonType == JoinCondition.comparisonOp.OP_EQ) {
                for (int j = i + 1; j < joinConditions.size(); j++) {
                    JoinCondition jcJ = joinConditions.get(j);
                    if (jcJ.usedVars == null) {
                        continue;
                    }
                    if (jcJ.comparisonType == JoinCondition.comparisonOp.OP_EQ && (jcJ.usedVars.size() == 2)
                            && (jcJ.numLeafInputs == 2) && (jcI.datasetBits == jcJ.datasetBits)) {
                        JCs.add(jcJ);
                    }
                }
                double sel = checkForPrimaryKey(JCs);
                //if (JCs.size() > 1) { // need at least two to form a composite join key
                // Now check if selectivities have to be adjusted for this composite key
                if (JCs.size() > 1) {
                    for (JoinCondition jc : JCs) {
                        jc.partOfComposite = true;
                    }
                }
                if (sel == -1.0) {
                    for (JoinCondition jc : JCs) {
                        jc.selectivity = stats.getSelectivityFromAnnotationMain(jc, null, true, false, jc.joinOp);
                    }

                } else {
                    for (JoinCondition jc : JCs) {
                        jc.selectivity = 1.0;
                    }
                    JCs.get(0).selectivity = sel; // store this in the first condition. Does not matter which one it is!
                }
            }
        }
    }

    // at this point the join is binary. Not sure if all the predicates may not be in the same order?? Appears to be the case
    private double checkForPrimaryKey(List<JoinCondition> jCs) {
        List<LogicalVariable> leftVars = new ArrayList<>();
        List<LogicalVariable> rightVars = new ArrayList<>();
        for (JoinCondition jc : jCs) {
            leftVars.add(jc.usedVars.get(0));
            rightVars.add(jc.usedVars.get(1));
        }
        double sel = -1.0;
        ILogicalOperator leftLeafInput = leafInputs.get(jCs.get(0).leftSide - 1);
        DataSourceScanOperator leftScanOp = findDataSourceScanOperator(leftLeafInput);
        boolean leftPrimary = false;
        if (leftScanOp.getVariables().containsAll(leftVars)
                && leftScanOp.getVariables().size() == leftVars.size() + 1) {
            // this is the primary side
            leftPrimary = true;
            sel = 1.0 / jnArray[jCs.get(0).leftSide].getOrigCardinality();
        }
        boolean rightPrimary = false;
        ILogicalOperator rightLeafInput = leafInputs.get(jCs.get(0).rightSide - 1);
        DataSourceScanOperator rightScanOp = findDataSourceScanOperator(rightLeafInput);
        if (rightScanOp.getVariables().containsAll(rightVars)
                && rightScanOp.getVariables().size() == rightVars.size() + 1) {
            // this is the primary side
            rightPrimary = true;
            sel = 1.0 / jnArray[jCs.get(0).rightSide].getOrigCardinality();
        }

        if (leftPrimary && rightPrimary) {
            // this is the subset case. The join cardinality will be the smaller side. So selectvity will be 1/biggerCard
            sel = 1.0 / Math.max(jnArray[jCs.get(0).leftSide].getOrigCardinality(),
                    jnArray[jCs.get(0).rightSide].getOrigCardinality());
        }
        return sel;
    }

    private boolean close(double size1, double size2) {
        double ratio = size1 / size2;
        if (ratio > 0.8 && ratio < 1.2) {
            return true;
        }
        return false;
    }

    private double smallerDatasetSize(int datasetBits) {
        double size = Cost.MAX_CARD;
        for (JoinNode jn : this.jnArray)
            if ((jn.datasetBits & datasetBits) > 0) {
                if (jn.origCardinality < size) {
                    size = jn.origCardinality;
                }
            }
        return size;
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

                    jn.datasetNames = new ArrayList<>();
                    jn.datasetNames.addAll(jnI.datasetNames);
                    jn.datasetNames.addAll(jnJ.datasetNames);

                    jn.aliases = new ArrayList<>();
                    jn.aliases.addAll(jnI.aliases);
                    jn.aliases.addAll(jnJ.aliases);

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
            DataSourceScanOperator scanOp = findDataSourceScanOperator(leafInput);
            if (scanOp != null && fakeLeafInputsMap.get(scanOp) != null) {
                jn.setFake();
            }
            int numArrayRefs = 0;
            if (unnestOpsInfo.size() > 0) {
                numArrayRefs = unnestOpsInfo.get(i - 1).size();
            }
            if (!cboTestMode) {
                if (idxDetails == null) {
                    dataScanPlan = jn.addSingleDatasetPlans();
                    if (dataScanPlan == PlanNode.NO_PLAN) {
                        return PlanNode.NO_PLAN;
                    }
                    continue;
                }
                // Compute the distinct cardinalities for each base join node.
                if (!jn.getFake()) {
                    jn.setCardsAndSizes(idxDetails, leafInput, i); // the fake case gets handled in this routine.
                }

                ILogicalOperator grpByDistinctOp = this.dataScanAndGroupByDistinctOps.get(scanOp);
                if (grpByDistinctOp != null) {
                    long distinctCardinality = stats.findDistinctCardinality(grpByDistinctOp);
                    jn.distinctCardinality = (double) distinctCardinality;
                    double grpInputCard = (double) Math.round(jn.cardinality * 100) / 100;
                    double grpOutputCard = (double) Math.round(Math.min(grpInputCard, distinctCardinality) * 100) / 100;
                    grpByDistinctOp.getAnnotations().put(OperatorAnnotations.OP_INPUT_CARDINALITY, grpInputCard);
                    grpByDistinctOp.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_CARDINALITY, grpOutputCard);
                }
            } else {
                // cboTestMode. There are no samples here.
                for (int j = 1; j <= numArrayRefs; j++) {
                    jn.setCardsAndSizesForFakeJn(i, j, 10.0);
                }
            }

            dataScanPlan = jn.addSingleDatasetPlans();
            if (dataScanPlan == PlanNode.NO_PLAN) {
                return PlanNode.NO_PLAN;
            }
            // We may not add any index plans, so need to check for NO_PLAN
            jn.addIndexAccessPlans(EnumerateJoinsRule.removeTrue(leafInput));
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

    protected boolean findUnnestOp(ILogicalOperator op) {
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
    // With this routine we can compute the cardinality of the join predicate between n1 and n2 correctly (2 in this case).
    //The predicate in Q7 between n1 and n2 is
    //(n1.name = INDIA AND n2.name = JAPAN) OR
    //(n1.name = JAPAN AND n2.name = INDIA)
    // So this appears as a join predicate but we have to compute the selectivities of the selection predicates inside the join. MESSY
    private void findSelectionPredsInsideJoins() throws AlgebricksException {
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
                    selOp = getStatsHandle().findSelectOpWithExpr(leafInput, exp);
                    if (selOp == null) {
                        selOp = (SelectOperator) leafInput;
                    }
                } else {
                    selOp = new SelectOperator(new MutableObject<>(exp));
                    selOp.getInputs().add(new MutableObject<>(leafInput));
                }
                sel = getStatsHandle().findSelectivityForThisPredicate(selOp, (AbstractFunctionCallExpression) exp,
                        findUnnestOp(selOp));
                // Sometimes the sample query returns greater more rows than the sample size. Cap the selectivity to 0.9999
                sel = Math.min(sel, 0.9999);

                // Add the selectivity annotation.
                PredicateCardinalityAnnotation anno = new PredicateCardinalityAnnotation(sel);
                AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) exp;
                afce.putAnnotation(anno);
            }
        }

        if (this.singleDatasetPreds.size() > 0) {
            for (JoinCondition jc : joinConditions) {
                // we may be repeating some work here, but that is ok. This will rarely happen (happens in q7 tpch)
                double sel = stats.getSelectivityFromAnnotationMain(jc, null, false, true, null);
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

        findJoinConditionsAndDoTC();
        addTCSelectionPredicates();
        keepOnlyOneSelectivityHint();
        int lastBaseLevelJnNum = enumerateBaseLevelJoinNodes();
        if (lastBaseLevelJnNum == PlanNode.NO_PLAN) {
            return PlanNode.NO_PLAN;
        }

        IPlanPrettyPrinter pp = optCtx.getPrettyPrinter();
        if (LOGGER.isTraceEnabled()) {
            EnumerateJoinsRule.printPlan(pp, op, "Original Whole plan in JN 1");
        }

        for (JoinCondition jc : joinConditions) {
            if (!((AbstractFunctionCallExpression) jc.joinCondition).getFunctionIdentifier()
                    .equals(AlgebricksBuiltinFunctions.EQ)) {
                jc.selectivity = stats.getSelectivityFromAnnotationMain(jc, null, true, false, jc.joinOp);
            }
        }

        findSelectionPredsInsideJoins(); // This was added to make TPCH Q7 work. cleanup/redo. difficult to debug currently
        findIfJoinGraphIsConnected();

        if (LOGGER.isTraceEnabled()) {
            EnumerateJoinsRule.printPlan(pp, op, "Original Whole plan in JN 2");
        }

        markCompositeJoinPredicates();
        for (JoinCondition jc : joinConditions) {
            if (jc.selectivity == -1) {// just in case we missed computing some selectivities perhaps because there were no keys found
                jc.selectivity = stats.getSelectivityFromAnnotationMain(jc, null, true, false, jc.joinOp);
            }
        }
        int lastJnNum = enumerateHigherLevelJoinNodes();
        JoinNode lastJn = jnArray[allTabsJnNum];
        // return the cheapest plan
        if (LOGGER.isTraceEnabled()) {
            EnumerateJoinsRule.printPlan(pp, op, "Original Whole plan in JN END");
            LOGGER.trace(dumpJoinNodes(lastJnNum));
        }

        return lastJn.cheapestPlanIndex;
    }

    private void keepOnlyOneSelectivityHint() {
        AbstractFunctionCallExpression afce;
        for (JoinCondition jc : joinConditions) {
            int n = 0;
            for (SelectOperator selOp : jc.derivedSelOps) {
                afce = (AbstractFunctionCallExpression) selOp.getCondition().getValue();
                if (afce.hasAnnotation(PredicateCardinalityAnnotation.class)) {
                    n++;
                }
            }
            if (n <= 1) { // R.a = S.a and R.a < 1
                return; // perfect. At most one predicate has the annotation
            } else {// n == 2, both of them have it of them have it, So remove it from the last one
                // R.a = S.a and R.a < 1 and S.a < 1; user typed in both predicates, so each one looks derived.
                afce = (AbstractFunctionCallExpression) jc.derivedSelOps.get(n - 1).getCondition().getValue();
                afce.removeAnnotation(PredicateCardinalityAnnotation.class);
            }
        }
    }

    // R.a = S.a and R.a op operand ==> S.a op operand
    private void addTCSelectionPredicates() throws AlgebricksException {
        List<SelectOperator> existingSelOps = new ArrayList<>();
        for (ILogicalOperator leafInput : this.leafInputs) {
            ILogicalOperator li = leafInput.getInputs().get(0).getValue(); // skip the true on the top
            List<SelectOperator> selOps = findAllSimpleSelOps(li); // variable op operand
            existingSelOps.addAll(selOps);
        }
        addTCSelectionPredicatesHelper(existingSelOps);
    }

    private void addTCSelectionPredicatesHelper(List<SelectOperator> existingSelOps) throws AlgebricksException {
        for (SelectOperator selOp : existingSelOps) {
            AbstractFunctionCallExpression exp = (AbstractFunctionCallExpression) selOp.getCondition().getValue();
            Mutable<ILogicalExpression> x = exp.getArguments().get(0);
            VariableReferenceExpression varRef = (VariableReferenceExpression) x.getValue();
            LogicalVariable var = varRef.getVariableReference();
            SelectOperator newSelOp;
            List<JoinCondition> jcs = findVarinJoinPreds(var);
            for (JoinCondition jc : jcs) { // join predicate can be R.a = S.a or S.a = R.a. Check for both cases
                if (var == jc.usedVars.get(0)) { // R.a
                    newSelOp = makeNewSelOper(jc, existingSelOps, jc.usedVars.get(1), // == S.a
                            ((AbstractFunctionCallExpression) selOp.getCondition().getValue()).getFunctionInfo(), // op
                            exp.getArguments().get(1)); // operand
                    if (newSelOp != null) { // does not already exist
                        addSelOpToLeafInput(jc, jc.usedVars.get(1), newSelOp);
                    }
                } else if (var == jc.usedVars.get(1)) { // R.a
                    newSelOp = makeNewSelOper(jc, existingSelOps, jc.usedVars.get(0), // == S.a
                            ((AbstractFunctionCallExpression) selOp.getCondition().getValue()).getFunctionInfo(), // op
                            exp.getArguments().get(1)); // operand
                    if (newSelOp != null) {
                        addSelOpToLeafInput(jc, jc.usedVars.get(0), newSelOp);
                    }
                }
            }
        }
    }

    private SelectOperator makeNewSelOper(JoinCondition jc, List<SelectOperator> existingSelOps, LogicalVariable var,
            IFunctionInfo tag, Mutable<ILogicalExpression> arg) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<>();
        VariableReferenceExpression e1 = new VariableReferenceExpression(var);
        arguments.add(new MutableObject<>(e1)); // S.a
        arguments.add(new MutableObject<>(arg.getValue())); // this will be the operand
        ScalarFunctionCallExpression expr = new ScalarFunctionCallExpression(tag, arguments); //S.a op operand
        SelectOperator newsel = new SelectOperator(new MutableObject<>(expr), null, null);
        if (newSelNotPresent(jc, newsel, existingSelOps)) {
            LOGGER.info("adding newsel " + newsel.getCondition());
            return newsel; // add since it does not exist
        } else {
            return null; // already exists, no need to add again
        }
    }

    private boolean newSelNotPresent(JoinCondition jc, SelectOperator newsel, List<SelectOperator> existingSelOps) {
        for (SelectOperator existingSelOp : existingSelOps) {
            if (newsel.getCondition().equals(existingSelOp.getCondition())) {
                PredicateCardinalityAnnotation anno = new PredicateCardinalityAnnotation(0.9999); // cannot be 1.0 as check in setCardsAndSizes will not work
                AbstractFunctionCallExpression afce =
                        (AbstractFunctionCallExpression) existingSelOp.getCondition().getValue();
                afce.putAnnotation(anno);
                jc.derivedSelOps.add(existingSelOp);
                return false;
            }
        }
        return true;
    }

    private void addSelOpToLeafInput(JoinCondition jc, LogicalVariable var, SelectOperator newSelOp)
            throws AlgebricksException {
        int l = varLeafInputIds.get(var); // get the corresponding leafInput using the map
        ILogicalOperator parent = leafInputs.get(l - 1);
        ILogicalOperator child = parent.getInputs().get(0).getValue();
        parent.getInputs().get(0).setValue(newSelOp);
        newSelOp.getInputs().add(new MutableObject<>(child));
        // Add the selectivity annotation with selectivity 1.0;
        // Note the actual cardinality will be different; but all join cardinalities should be ok.
        PredicateCardinalityAnnotation anno = new PredicateCardinalityAnnotation(0.9999);
        AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) newSelOp.getCondition().getValue();
        afce.putAnnotation(anno);
        jc.derivedSelOps.add(newSelOp);
        optCtx.computeAndSetTypeEnvironmentForOperator(newSelOp);
    }

    private List<JoinCondition> findVarinJoinPreds(LogicalVariable var) {
        List<JoinCondition> jcs = new ArrayList<>();
        for (JoinCondition jc : joinConditions) {
            if (jc.usedVars != null && jc.usedVars.contains(var)) { // this will only search inner join predicates
                jcs.add(jc);
            }
        }
        return jcs;
    }

    private List<SelectOperator> findAllSimpleSelOps(ILogicalOperator li) {
        List<SelectOperator> selOps = new ArrayList<>();
        while (li != null && li.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (li.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                SelectOperator selOp = (SelectOperator) li;
                ILogicalExpression condition = selOp.getCondition().getValue();
                if (simpleCondition(condition)) {
                    selOps.add(selOp);
                }
            }
            li = li.getInputs().get(0).getValue();
        }
        return selOps;
    }

    private boolean simpleCondition(ILogicalExpression condition) {
        if (condition.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression exp = (AbstractFunctionCallExpression) condition;
            if (exp.getArguments().size() == 2) {
                Mutable<ILogicalExpression> arg0 = exp.getArguments().get(0);
                Mutable<ILogicalExpression> arg1 = exp.getArguments().get(1);
                if (arg0.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE
                        && arg1.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    return true;
                }
            }
        }
        return false;
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
        sb.append("\n\nCBO CONTEXT").append('\n');
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
