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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.algebra.operators.physical.IntervalIndexJoinPOperator;
import org.apache.asterix.algebra.operators.physical.IntervalLocalRangeSplitterPOperator;
import org.apache.asterix.algebra.operators.physical.IntervalPartitionJoinPOperator;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator.JoinPartitioningType;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.MaterializePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.MergeJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RangePartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.UnionAllPOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangePartitionType.RangePartitioningType;

/**
 * Before:
 *
 * <pre>
 *
 * Left
 *
 *
 * Right
 * </pre>
 *
 * After:
 *
 * <pre>
 *
 * Left
 *
 *
 * Right
 * </pre>
 */
public class IntervalSplitPartitioningRule implements IAlgebraicRewriteRule {

    private static final int LEFT = 0;
    private static final int RIGHT = 1;

    private static final int START_SPLITS = 3;

    private static final Set<FunctionIdentifier> INTERVAL_JOIN_CONDITIONS = new HashSet<>();
    static {
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_AFTER);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_BEFORE);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_COVERED_BY);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_COVERS);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_ENDED_BY);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_ENDS);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_MEETS);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_MET_BY);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_OVERLAPPED_BY);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_OVERLAPPING);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_OVERLAPS);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_STARTED_BY);
        INTERVAL_JOIN_CONDITIONS.add(AsterixBuiltinFunctions.INTERVAL_STARTS);
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        if (!isIntervalJoin(op)) {
            return false;
        }
        InnerJoinOperator originalIntervalJoin = (InnerJoinOperator) op;
        ExecutionMode mode = originalIntervalJoin.getExecutionMode();
        Set<LogicalVariable> localLiveVars = new ListSet<>();
        VariableUtilities.getLiveVariables(originalIntervalJoin, localLiveVars);

        Mutable<ILogicalOperator> leftSortedInput = originalIntervalJoin.getInputs().get(0);
        Mutable<ILogicalOperator> rightSortedInput = originalIntervalJoin.getInputs().get(1);
        if (leftSortedInput.getValue().getOperatorTag() != LogicalOperatorTag.EXCHANGE
                && rightSortedInput.getValue().getOperatorTag() != LogicalOperatorTag.EXCHANGE) {
            return false;
        }

        Mutable<ILogicalOperator> leftSorter = leftSortedInput.getValue().getInputs().get(0);
        Mutable<ILogicalOperator> rightSorter = rightSortedInput.getValue().getInputs().get(0);
        if (leftSorter.getValue().getOperatorTag() != LogicalOperatorTag.ORDER
                && rightSorter.getValue().getOperatorTag() != LogicalOperatorTag.ORDER) {
            return false;
        }
        LogicalVariable leftSortKey = getSortKey(leftSorter.getValue());
        LogicalVariable rightSortKey = getSortKey(rightSorter.getValue());
        if (leftSortKey == null || rightSortKey == null) {
            return false;
        }

        Mutable<ILogicalOperator> leftRangeInput = leftSorter.getValue().getInputs().get(0);
        Mutable<ILogicalOperator> rightRangeInput = rightSorter.getValue().getInputs().get(0);
        IRangeMap leftRangeMap = getRangeMapForBranch(leftRangeInput.getValue());
        IRangeMap rightRangeMap = getRangeMapForBranch(rightRangeInput.getValue());
        if (leftRangeMap == null || rightRangeMap == null) {
            return false;
        }
        // TODO check physical join

        // Interval local partition operators
        LogicalVariable leftJoinKey = getJoinKey(originalIntervalJoin.getCondition().getValue(), LEFT);
        LogicalVariable rightJoinKey = getJoinKey(originalIntervalJoin.getCondition().getValue(), RIGHT);
        if (leftJoinKey == null || rightJoinKey == null) {
            return false;
        }
        ReplicateOperator leftIntervalSplit = getIntervalSplitOperator(leftSortKey, leftRangeMap, mode);
        Mutable<ILogicalOperator> leftIntervalSplitRef = new MutableObject<>(leftIntervalSplit);
        ReplicateOperator rightIntervalSplit = getIntervalSplitOperator(rightSortKey, rightRangeMap, mode);
        Mutable<ILogicalOperator> rightIntervalSplitRef = new MutableObject<>(rightIntervalSplit);

        // Replicate operators
        ReplicateOperator leftStartsSplit = getReplicateOperator(START_SPLITS, mode);
        Mutable<ILogicalOperator> leftStartsSplitRef = new MutableObject<>(leftStartsSplit);
        ReplicateOperator rightStartsSplit = getReplicateOperator(START_SPLITS, mode);
        Mutable<ILogicalOperator> rightStartsSplitRef = new MutableObject<>(rightStartsSplit);

        // Covers Join Operator
        ILogicalOperator leftCoversJoin = getNestedLoop(originalIntervalJoin.getCondition(), context, mode);
        Mutable<ILogicalOperator> leftCoversJoinRef = new MutableObject<>(leftCoversJoin);
        ILogicalOperator rightCoversJoin = getNestedLoop(originalIntervalJoin.getCondition(), context, mode);
        Mutable<ILogicalOperator> rightCoversJoinRef = new MutableObject<>(rightCoversJoin);

        // Ends Join Operator
        ILogicalOperator startsJoin = getIntervalJoin(originalIntervalJoin, context, mode);
        ILogicalOperator leftEndsJoin = getIntervalJoin(originalIntervalJoin, context, mode);
        ILogicalOperator rightEndsJoin = getIntervalJoin(originalIntervalJoin, context, mode);
        if (startsJoin == null || leftEndsJoin == null || rightEndsJoin == null) {
            return false;
        }
        Mutable<ILogicalOperator> startsJoinRef = new MutableObject<>(startsJoin);
        Mutable<ILogicalOperator> leftEndsJoinRef = new MutableObject<>(leftEndsJoin);
        Mutable<ILogicalOperator> rightEndsJoinRef = new MutableObject<>(rightEndsJoin);

        // Materialize Operator
        ILogicalOperator leftMaterialize0 = getMaterializeOperator(mode);
        Mutable<ILogicalOperator> leftMaterialize0Ref = new MutableObject<>(leftMaterialize0);
        ILogicalOperator leftMaterialize1 = getMaterializeOperator(mode);
        Mutable<ILogicalOperator> leftMaterialize1Ref = new MutableObject<>(leftMaterialize1);
        ILogicalOperator leftMaterialize2 = getMaterializeOperator(mode);
        Mutable<ILogicalOperator> leftMaterialize2Ref = new MutableObject<>(leftMaterialize2);
        ILogicalOperator rightMaterialize0 = getMaterializeOperator(mode);
        Mutable<ILogicalOperator> rightMaterialize0Ref = new MutableObject<>(rightMaterialize0);
        ILogicalOperator rightMaterialize1 = getMaterializeOperator(mode);
        Mutable<ILogicalOperator> rightMaterialize1Ref = new MutableObject<>(rightMaterialize1);
        ILogicalOperator rightMaterialize2 = getMaterializeOperator(mode);
        Mutable<ILogicalOperator> rightMaterialize2Ref = new MutableObject<>(rightMaterialize2);

        // Union All Operator
        ILogicalOperator union1 = getUnionOperator(localLiveVars, mode);
        Mutable<ILogicalOperator> union1Ref = new MutableObject<>(union1);
        ILogicalOperator union2 = getUnionOperator(localLiveVars, mode);
        Mutable<ILogicalOperator> union2Ref = new MutableObject<>(union2);
        ILogicalOperator union3 = getUnionOperator(localLiveVars, mode);
        Mutable<ILogicalOperator> union3Ref = new MutableObject<>(union3);
        ILogicalOperator union4 = getUnionOperator(localLiveVars, mode);
        Mutable<ILogicalOperator> union4Ref = new MutableObject<>(union4);

        // Remove old path
        originalIntervalJoin.getInputs().clear();

        // Connect main path
        connectOperators(leftIntervalSplitRef, leftSortedInput, context);
        context.computeAndSetTypeEnvironmentForOperator(leftIntervalSplitRef.getValue());
        connectOperators(leftMaterialize0Ref, leftIntervalSplitRef, context);
        context.computeAndSetTypeEnvironmentForOperator(leftMaterialize0Ref.getValue());
        connectOperators(leftMaterialize1Ref, leftIntervalSplitRef, context);
        context.computeAndSetTypeEnvironmentForOperator(leftMaterialize1Ref.getValue());
        connectOperators(leftMaterialize2Ref, leftIntervalSplitRef, context);
        context.computeAndSetTypeEnvironmentForOperator(leftMaterialize2Ref.getValue());

        connectOperators(leftStartsSplitRef, leftMaterialize0Ref, context);
        context.computeAndSetTypeEnvironmentForOperator(leftStartsSplitRef.getValue());

        connectOperators(rightIntervalSplitRef, rightSortedInput, context);
        context.computeAndSetTypeEnvironmentForOperator(rightIntervalSplitRef.getValue());
        connectOperators(rightMaterialize0Ref, rightIntervalSplitRef, context);
        context.computeAndSetTypeEnvironmentForOperator(rightMaterialize0Ref.getValue());
        connectOperators(rightMaterialize1Ref, rightIntervalSplitRef, context);
        context.computeAndSetTypeEnvironmentForOperator(rightMaterialize1Ref.getValue());
        connectOperators(rightMaterialize2Ref, rightIntervalSplitRef, context);
        context.computeAndSetTypeEnvironmentForOperator(rightMaterialize2Ref.getValue());

        connectOperators(rightStartsSplitRef, rightMaterialize0Ref, context);
        context.computeAndSetTypeEnvironmentForOperator(rightStartsSplitRef.getValue());

        // Connect left and right starts path
        connectOperators(startsJoinRef, leftStartsSplitRef, context);
        connectOperators(startsJoinRef, rightStartsSplitRef, context);
        context.computeAndSetTypeEnvironmentForOperator(startsJoinRef.getValue());

        // Connect left ends path
        connectOperators(leftEndsJoinRef, leftMaterialize1Ref, context);
        connectOperators(leftEndsJoinRef, rightStartsSplitRef, context);
        context.computeAndSetTypeEnvironmentForOperator(leftEndsJoinRef.getValue());
        connectOperators(union1Ref, startsJoinRef, context);
        connectOperators(union1Ref, leftEndsJoinRef, context);
        context.computeAndSetTypeEnvironmentForOperator(union1Ref.getValue());

        // Connect left covers path
        connectOperators(leftCoversJoinRef, leftMaterialize2Ref, context);
        connectOperators(leftCoversJoinRef, rightStartsSplitRef, context);
        context.computeAndSetTypeEnvironmentForOperator(leftCoversJoinRef.getValue());
        connectOperators(union2Ref, union1Ref, context);
        connectOperators(union2Ref, leftCoversJoinRef, context);
        context.computeAndSetTypeEnvironmentForOperator(union2Ref.getValue());

        // Connect right ends path
        connectOperators(rightEndsJoinRef, leftStartsSplitRef, context);
        connectOperators(rightEndsJoinRef, rightMaterialize1Ref, context);
        context.computeAndSetTypeEnvironmentForOperator(rightEndsJoinRef.getValue());
        connectOperators(union3Ref, union2Ref, context);
        connectOperators(union3Ref, rightEndsJoinRef, context);
        context.computeAndSetTypeEnvironmentForOperator(union3Ref.getValue());

        // Connect right covers path
        connectOperators(rightCoversJoinRef, leftStartsSplitRef, context);
        connectOperators(rightCoversJoinRef, rightMaterialize2Ref, context);
        context.computeAndSetTypeEnvironmentForOperator(rightCoversJoinRef.getValue());
        connectOperators(union4Ref, union3Ref, context);
        connectOperators(union4Ref, rightCoversJoinRef, context);
        context.computeAndSetTypeEnvironmentForOperator(union4Ref.getValue());

        // Update context
        opRef.setValue(union4Ref.getValue());

        context.addToDontApplySet(this, startsJoin);
        context.addToDontApplySet(this, leftCoversJoin);
        context.addToDontApplySet(this, rightCoversJoin);
        context.addToDontApplySet(this, leftCoversJoin);
        context.addToDontApplySet(this, rightEndsJoin);

        context.addToDontApplySet(this, union1);
        context.addToDontApplySet(this, union2);
        context.addToDontApplySet(this, union3);
        context.addToDontApplySet(this, union4);
        return true;
    }

    private LogicalVariable getSortKey(ILogicalOperator op) {
        if (op.getOperatorTag() != LogicalOperatorTag.ORDER) {
            return null;
        }
        OrderOperator oo = (OrderOperator) op;
        List<Pair<IOrder, Mutable<ILogicalExpression>>> order = oo.getOrderExpressions();
        Mutable<ILogicalExpression> sortLe = order.get(0).second;
        if (sortLe.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return ((VariableReferenceExpression) sortLe.getValue()).getVariableReference();
        }
        return null;
    }

    private LogicalVariable getJoinKey(ILogicalExpression expr, int branch) {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        // Check whether the function is a function we want to alter.
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (!INTERVAL_JOIN_CONDITIONS.contains(funcExpr.getFunctionIdentifier())) {
            return null;
        }
        ILogicalExpression funcArg = funcExpr.getArguments().get(branch).getValue();
        if (funcArg instanceof VariableReferenceExpression) {
            return ((VariableReferenceExpression) funcArg).getVariableReference();
        }
        return null;
    }

    private void connectOperators(Mutable<ILogicalOperator> child, Mutable<ILogicalOperator> parent,
            IOptimizationContext context) throws AlgebricksException {
        if (parent.getValue().getOperatorTag() != LogicalOperatorTag.EXCHANGE) {
            ILogicalOperator eo = getExchangeOperator(child.getValue().getExecutionMode());
            Mutable<ILogicalOperator> eoRef = new MutableObject<>(eo);
            eo.getInputs().add(parent);
            if (parent.getValue().getOperatorTag() == LogicalOperatorTag.REPLICATE) {
                ReplicateOperator ro = (ReplicateOperator) parent.getValue();
                ro.getOutputs().add(eoRef);
            }
            child.getValue().getInputs().add(eoRef);
            context.computeAndSetTypeEnvironmentForOperator(eo);
            context.computeAndSetTypeEnvironmentForOperator(child.getValue());
        } else {
            if (parent.getValue().getOperatorTag() == LogicalOperatorTag.REPLICATE) {
                ReplicateOperator ro = (ReplicateOperator) parent.getValue();
                ro.getOutputs().add(child);
            }
            child.getValue().getInputs().add(parent);
            context.computeAndSetTypeEnvironmentForOperator(child.getValue());
        }
    }

    private ILogicalOperator getExchangeOperator(ExecutionMode mode) {
        ExchangeOperator eo = new ExchangeOperator();
        eo.setPhysicalOperator(new OneToOneExchangePOperator());
        eo.setExecutionMode(mode);
        return eo;
    }

    private ReplicateOperator getIntervalSplitOperator(LogicalVariable key, IRangeMap rangeMap, ExecutionMode mode) {
        List<LogicalVariable> joinKeyLogicalVars = new ArrayList<>();
        joinKeyLogicalVars.add(key);
        //create the logical and physical operator
        boolean[] flags = new boolean[2];
        for (int i = 0; i < flags.length; ++i) {
            flags[i] = true;
        }
        ReplicateOperator splitOperator = new ReplicateOperator(flags.length, flags);
        IntervalLocalRangeSplitterPOperator splitPOperator = new IntervalLocalRangeSplitterPOperator(joinKeyLogicalVars,
                rangeMap);
        splitOperator.setPhysicalOperator(splitPOperator);
        splitOperator.setExecutionMode(mode);
        return splitOperator;
    }

    private ReplicateOperator getReplicateOperator(int outputArity, ExecutionMode mode) {
        boolean[] flags = new boolean[outputArity];
        for (int i = 0; i < flags.length; ++i) {
            flags[i] = true;
        }
        ReplicateOperator ro = new ReplicateOperator(flags.length, flags);
        ReplicatePOperator rpo = new ReplicatePOperator();
        ro.setPhysicalOperator(rpo);
        ro.setExecutionMode(mode);
        return ro;
    }

    private ILogicalOperator getMaterializeOperator(ExecutionMode mode) {
        MaterializeOperator mo = new MaterializeOperator();
        MaterializePOperator mpo = new MaterializePOperator(false);
        mo.setPhysicalOperator(mpo);
        mo.setExecutionMode(mode);
        return mo;
    }

    private ILogicalOperator getNestedLoop(Mutable<ILogicalExpression> condition, IOptimizationContext context,
            ExecutionMode mode) {
        int memoryJoinSize = context.getPhysicalOptimizationConfig().getMaxFramesForJoin();
        InnerJoinOperator ijo = new InnerJoinOperator(condition);
        NestedLoopJoinPOperator nljpo = new NestedLoopJoinPOperator(JoinKind.INNER, JoinPartitioningType.BROADCAST,
                memoryJoinSize);
        ijo.setPhysicalOperator(nljpo);
        ijo.setExecutionMode(mode);
        return ijo;
    }

    private ILogicalOperator getIntervalJoin(ILogicalOperator op, IOptimizationContext context, ExecutionMode mode) {
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return null;
        }
        InnerJoinOperator ijo = (InnerJoinOperator) op;
        InnerJoinOperator ijoClone = new InnerJoinOperator(ijo.getCondition());

        int memoryJoinSize = context.getPhysicalOptimizationConfig().getMaxFramesForJoin();
        IPhysicalOperator joinPo = ijo.getPhysicalOperator();
        if (joinPo.getOperatorTag() == PhysicalOperatorTag.MERGE_JOIN) {
            MergeJoinPOperator mjpo = (MergeJoinPOperator) joinPo;
            MergeJoinPOperator mjpoClone = new MergeJoinPOperator(mjpo.getKind(), mjpo.getPartitioningType(),
                    mjpo.getKeysLeftBranch(), mjpo.getKeysRightBranch(), memoryJoinSize,
                    mjpo.getMergeJoinCheckerFactory(), mjpo.getRangeMap());
            ijoClone.setPhysicalOperator(mjpoClone);
        } else if (joinPo.getOperatorTag() == PhysicalOperatorTag.EXTENSION_OPERATOR) {
            if (joinPo instanceof IntervalIndexJoinPOperator) {
                IntervalIndexJoinPOperator iijpo = (IntervalIndexJoinPOperator) joinPo;
                IntervalIndexJoinPOperator iijpoClone = new IntervalIndexJoinPOperator(iijpo.getKind(),
                        iijpo.getPartitioningType(), iijpo.getKeysLeftBranch(), iijpo.getKeysRightBranch(),
                        memoryJoinSize, iijpo.getIntervalMergeJoinCheckerFactory(), iijpo.getRangeMap());
                ijoClone.setPhysicalOperator(iijpoClone);
            } else if (joinPo instanceof IntervalPartitionJoinPOperator) {
                IntervalPartitionJoinPOperator ipjpo = (IntervalPartitionJoinPOperator) joinPo;
                IntervalPartitionJoinPOperator iijpoClone = new IntervalPartitionJoinPOperator(ipjpo.getKind(),
                        ipjpo.getPartitioningType(), ipjpo.getKeysLeftBranch(), ipjpo.getKeysRightBranch(),
                        memoryJoinSize, ipjpo.getBuildTupleCount(), ipjpo.getProbeTupleCount(),
                        ipjpo.getBuildMaxDuration(), ipjpo.getProbeMaxDuration(), ipjpo.getAvgTuplesInFrame(),
                        ipjpo.getIntervalMergeJoinCheckerFactory(), ipjpo.getRangeMap());
                ijoClone.setPhysicalOperator(iijpoClone);
            } else {
                return null;
            }
        } else {
            return null;
        }
        ijoClone.setExecutionMode(mode);
        return ijoClone;
    }

    private ILogicalOperator getUnionOperator(Set<LogicalVariable> localLiveVars, ExecutionMode mode) {
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap = new ArrayList<>();
        for (LogicalVariable lv : localLiveVars) {
            varMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(lv, lv, lv));
        }
        UnionAllOperator uao = new UnionAllOperator(varMap);
        uao.setPhysicalOperator(new UnionAllPOperator());
        uao.setExecutionMode(mode);
        return uao;
    }

    private boolean isIntervalJoin(ILogicalOperator op) {
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }
        // TODO add check for condition.
        InnerJoinOperator ijo = (InnerJoinOperator) op;
        if (ijo.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.MERGE_JOIN) {
            return true;
        }
        if (ijo.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.EXTENSION_OPERATOR) {
            return true;
        }
        return false;
    }

    private IRangeMap getRangeMapForBranch(ILogicalOperator op) {
        if (op.getOperatorTag() != LogicalOperatorTag.EXCHANGE) {
            return null;
        }
        ExchangeOperator exchangeLeft = (ExchangeOperator) op;
        if (exchangeLeft.getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.RANGE_PARTITION_EXCHANGE) {
            return null;
        }
        RangePartitionExchangePOperator exchangeLeftPO = (RangePartitionExchangePOperator) exchangeLeft
                .getPhysicalOperator();
        if (exchangeLeftPO.getRangeType() != RangePartitioningType.SPLIT) {
            return null;
        }
        return exchangeLeftPO.getRangeMap();
    }

}
