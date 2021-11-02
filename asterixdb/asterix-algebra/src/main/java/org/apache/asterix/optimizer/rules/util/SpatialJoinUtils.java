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
package org.apache.asterix.optimizer.rules.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.asterix.algebra.operators.physical.SpatialJoinPOperator;
import org.apache.asterix.common.annotations.SpatialJoinAnnotation;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.joins.spatial.utils.ISpatialJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.spatial.utils.IntersectSpatialJoinUtilFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.BroadcastExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.UnnestPOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class SpatialJoinUtils {

    private static final int DEFAULT_ROWS = 100;
    private static final int DEFAULT_COLUMNS = 100;

    protected static boolean trySpatialJoinAssignment(AbstractBinaryJoinOperator op, IOptimizationContext context,
            ILogicalExpression joinCondition, int left, int right) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) joinCondition;
        // Check if the join condition contains spatial join
        AbstractFunctionCallExpression spatialJoinFuncExpr = null;
        // Maintain conditions which is not spatial_intersect in the join condition
        List<Mutable<ILogicalExpression>> conditionExprs = new ArrayList<>();

        if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.AND)) {
            // Join condition contains multiple conditions along with spatial_intersect
            List<Mutable<ILogicalExpression>> inputExprs = funcExpr.getArguments();
            if (inputExprs.size() == 0) {
                return false;
            }

            boolean spatialIntersectExists = false;
            for (Mutable<ILogicalExpression> exp : inputExprs) {
                AbstractFunctionCallExpression funcCallExp = (AbstractFunctionCallExpression) exp.getValue();
                if (funcCallExp.getFunctionIdentifier().equals(BuiltinFunctions.SPATIAL_INTERSECT)) {
                    spatialJoinFuncExpr = funcCallExp;
                    spatialIntersectExists = true;
                } else {
                    // Retain the other conditions
                    conditionExprs.add(exp);
                }
            }

            if (!spatialIntersectExists) {
                return false;
            }
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.SPATIAL_INTERSECT)) {
            // Join condition is spatial_intersect only
            spatialJoinFuncExpr = funcExpr;
        } else {
            return false;
        }

        // Apply the PBSM join algorithm with/without hint
        SpatialJoinAnnotation spatialJoinAnn = spatialJoinFuncExpr.getAnnotation(SpatialJoinAnnotation.class);
        return SpatialJoinUtils.updateJoinPlan(op, spatialJoinFuncExpr, conditionExprs, spatialJoinAnn, context, left,
                right);
    }

    private static void setSpatialJoinOp(AbstractBinaryJoinOperator op, List<LogicalVariable> keysLeftBranch,
            List<LogicalVariable> keysRightBranch, IOptimizationContext context) throws AlgebricksException {
        ISpatialJoinUtilFactory isjuf = new IntersectSpatialJoinUtilFactory();
        op.setPhysicalOperator(new SpatialJoinPOperator(op.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.PAIRWISE, keysLeftBranch, keysRightBranch,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), isjuf));
        op.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(op);
    }

    private static LogicalVariable injectSpatialTileUnnestOperator(IOptimizationContext context,
            Mutable<ILogicalOperator> op, LogicalVariable unnestVar, Mutable<ILogicalExpression> unnestMBRExpr,
            int numRows, int numColumns) throws AlgebricksException {
        SourceLocation srcLoc = op.getValue().getSourceLocation();
        LogicalVariable tileIdVar = context.newVar();
        VariableReferenceExpression unnestVarRef = new VariableReferenceExpression(unnestVar);
        unnestVarRef.setSourceLocation(srcLoc);
        UnnestingFunctionCallExpression spatialTileFuncExpr = new UnnestingFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_TILE),
                new MutableObject<>(unnestVarRef), unnestMBRExpr,
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(numRows)))),
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(numColumns)))));
        spatialTileFuncExpr.setSourceLocation(srcLoc);
        UnnestOperator unnestOp = new UnnestOperator(tileIdVar, new MutableObject<>(spatialTileFuncExpr));
        unnestOp.setPhysicalOperator(new UnnestPOperator());
        unnestOp.setSourceLocation(srcLoc);
        unnestOp.getInputs().add(new MutableObject<>(op.getValue()));
        context.computeAndSetTypeEnvironmentForOperator(unnestOp);
        unnestOp.recomputeSchema();
        op.setValue(unnestOp);

        return tileIdVar;
    }

    protected static boolean updateJoinPlan(AbstractBinaryJoinOperator op,
            AbstractFunctionCallExpression spatialJoinFuncExpr, List<Mutable<ILogicalExpression>> conditionExprs,
            SpatialJoinAnnotation spatialJoinAnn, IOptimizationContext context, int left, int right)
            throws AlgebricksException {
        // Extracts spatial intersect function's arguments
        List<Mutable<ILogicalExpression>> spatialJoinArgs = spatialJoinFuncExpr.getArguments();
        if (spatialJoinArgs.size() != 2) {
            return false;
        }

        ILogicalExpression spatialJoinLeftArg = spatialJoinArgs.get(left).getValue();
        ILogicalExpression spatialJoinRightArg = spatialJoinArgs.get(right).getValue();

        // Left and right arguments of the spatial_intersect function should be variables
        if (spatialJoinLeftArg.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || spatialJoinRightArg.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }

        // We only apply this rule if the arguments of spatial_intersect are ARectangle
        IVariableTypeEnvironment typeEnvironment = op.computeInputTypeEnvironment(context);
        IAType leftType = (IAType) context.getExpressionTypeComputer().getType(spatialJoinLeftArg,
                context.getMetadataProvider(), typeEnvironment);
        IAType rightType = (IAType) context.getExpressionTypeComputer().getType(spatialJoinRightArg,
                context.getMetadataProvider(), typeEnvironment);
        if ((leftType.getTypeTag() != BuiltinType.ARECTANGLE.getTypeTag())
                || (rightType.getTypeTag() != BuiltinType.ARECTANGLE.getTypeTag())) {
            return false;
        }

        // Gets both input branches of the spatial join.
        Mutable<ILogicalOperator> leftInputOp = op.getInputs().get(left);
        Mutable<ILogicalOperator> rightInputOp = op.getInputs().get(right);

        // Extract left and right variable of the predicate
        LogicalVariable spatialJoinVar0 = ((VariableReferenceExpression) spatialJoinLeftArg).getVariableReference();
        LogicalVariable spatialJoinVar1 = ((VariableReferenceExpression) spatialJoinRightArg).getVariableReference();

        LogicalVariable leftInputVar;
        LogicalVariable rightInputVar;
        Collection<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(leftInputOp.getValue(), liveVars);
        if (liveVars.contains(spatialJoinVar0)) {
            leftInputVar = spatialJoinVar0;
            rightInputVar = spatialJoinVar1;
        } else {
            leftInputVar = spatialJoinVar1;
            rightInputVar = spatialJoinVar0;
        }

        // If the hint is not provided, the intersection MBR of two inputs will be computed on the run time
        if (spatialJoinAnn == null) {
            buildSpatialJoinPlanWithDynamicMbr(op, context, spatialJoinFuncExpr, conditionExprs, leftInputOp,
                    rightInputOp, leftInputVar, rightInputVar);
        } else {
            buildSpatialJoinPlanWithStaticMbr(op, context, spatialJoinFuncExpr, conditionExprs, leftInputOp,
                    rightInputOp, leftInputVar, rightInputVar, spatialJoinAnn);
        }

        return true;
    }

    private static void buildSpatialJoinPlanWithStaticMbr(AbstractBinaryJoinOperator op, IOptimizationContext context,
            AbstractFunctionCallExpression spatialJoinFuncExpr, List<Mutable<ILogicalExpression>> conditionExprs,
            Mutable<ILogicalOperator> leftInputOp, Mutable<ILogicalOperator> rightInputOp, LogicalVariable leftInputVar,
            LogicalVariable rightInputVar, SpatialJoinAnnotation spatialJoinAnn) throws AlgebricksException {
        Mutable<ILogicalExpression> leftIntersectionMBRExpr = createRectangleExpression(spatialJoinAnn);
        Mutable<ILogicalExpression> rightIntersectionMBRExpr = createRectangleExpression(spatialJoinAnn);
        Mutable<ILogicalExpression> referencePointTestMBRExpr = createRectangleExpression(spatialJoinAnn);
        int numRows = spatialJoinAnn.getNumRows();
        int numColumns = spatialJoinAnn.getNumColumns();

        // Inject unnest operator to add tile ID to the left and right branch of the join operator
        LogicalVariable leftTileIdVar = SpatialJoinUtils.injectSpatialTileUnnestOperator(context, leftInputOp,
                leftInputVar, leftIntersectionMBRExpr, numRows, numColumns);
        LogicalVariable rightTileIdVar = SpatialJoinUtils.injectSpatialTileUnnestOperator(context, rightInputOp,
                rightInputVar, rightIntersectionMBRExpr, numRows, numColumns);

        // The reference point test condition is considered as a part of spatial join conditions if a hint is provided.
        ScalarFunctionCallExpression referenceIdEquiJoinCondition =
                createReferencePointTestCondition(op, referencePointTestMBRExpr, leftTileIdVar, rightTileIdVar,
                        leftInputVar, rightInputVar, numRows, numColumns);
        conditionExprs.add(new MutableObject<>(referenceIdEquiJoinCondition));

        conditionExprs.add(new MutableObject<>(spatialJoinFuncExpr));

        ScalarFunctionCallExpression updatedJoinCondition = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND), conditionExprs);
        updatedJoinCondition.setSourceLocation(op.getSourceLocation());
        Mutable<ILogicalExpression> joinConditionRef = op.getCondition();
        joinConditionRef.setValue(updatedJoinCondition);

        List<LogicalVariable> keysLeftBranch = new ArrayList<>();
        keysLeftBranch.add(leftTileIdVar);
        keysLeftBranch.add(leftInputVar);

        List<LogicalVariable> keysRightBranch = new ArrayList<>();
        keysRightBranch.add(rightTileIdVar);
        keysRightBranch.add(rightInputVar);

        SpatialJoinUtils.setSpatialJoinOp(op, keysLeftBranch, keysRightBranch, context);
    }

    private static void buildSpatialJoinPlanWithDynamicMbr(AbstractBinaryJoinOperator op, IOptimizationContext context,
            AbstractFunctionCallExpression spatialJoinFuncExpr, List<Mutable<ILogicalExpression>> conditionExprs,
            Mutable<ILogicalOperator> leftInputOp, Mutable<ILogicalOperator> rightInputOp, LogicalVariable leftInputVar,
            LogicalVariable rightInputVar) throws AlgebricksException {
        // Add a dynamic workflow to compute MBR of the left branch
        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> leftMBRCalculator =
                createDynamicMBRCalculator(op, context, leftInputOp, leftInputVar);
        MutableObject<ILogicalOperator> leftGlobalAgg = leftMBRCalculator.first;
        List<LogicalVariable> leftGlobalAggResultVars = leftMBRCalculator.second;
        MutableObject<ILogicalOperator> leftExchToJoinOpRef = leftMBRCalculator.third;
        LogicalVariable leftMBRVar = leftGlobalAggResultVars.get(0);

        // Add a dynamic workflow to compute MBR of the right branch
        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> rightMBRCalculator =
                createDynamicMBRCalculator(op, context, rightInputOp, rightInputVar);
        MutableObject<ILogicalOperator> rightGlobalAgg = rightMBRCalculator.first;
        List<LogicalVariable> rightGlobalAggResultVars = rightMBRCalculator.second;
        MutableObject<ILogicalOperator> rightExchToJoinOpRef = rightMBRCalculator.third;
        LogicalVariable rightMBRVar = rightGlobalAggResultVars.get(0);

        // Join the left and right union MBR
        Mutable<ILogicalExpression> trueCondition =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)));
        InnerJoinOperator unionMBRJoinOp = new InnerJoinOperator(trueCondition, leftGlobalAgg, rightGlobalAgg);
        unionMBRJoinOp.setSourceLocation(op.getSourceLocation());
        unionMBRJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER,
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        MutableObject<ILogicalOperator> unionMBRJoinOpRef = new MutableObject<>(unionMBRJoinOp);
        unionMBRJoinOp.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(unionMBRJoinOp);

        // Compute the intersection rectangle of left MBR and right MBR
        List<Mutable<ILogicalExpression>> getIntersectionFuncInputExprs = new ArrayList<>();
        getIntersectionFuncInputExprs.add(new MutableObject<>(new VariableReferenceExpression(leftMBRVar)));
        getIntersectionFuncInputExprs.add(new MutableObject<>(new VariableReferenceExpression(rightMBRVar)));
        ScalarFunctionCallExpression getIntersectionFuncExpr = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.GET_INTERSECTION),
                getIntersectionFuncInputExprs);
        getIntersectionFuncExpr.setSourceLocation(op.getSourceLocation());

        Mutable<ILogicalExpression> intersectionMBRExpr = new MutableObject<>(getIntersectionFuncExpr);
        LogicalVariable intersectionMBR = context.newVar();
        AbstractLogicalOperator intersectionMBRAssignOperator =
                new AssignOperator(intersectionMBR, intersectionMBRExpr);
        intersectionMBRAssignOperator.setSourceLocation(op.getSourceLocation());
        intersectionMBRAssignOperator.setExecutionMode(op.getExecutionMode());
        intersectionMBRAssignOperator.setPhysicalOperator(new AssignPOperator());
        intersectionMBRAssignOperator.getInputs().add(new MutableObject<>(unionMBRJoinOpRef.getValue()));
        context.computeAndSetTypeEnvironmentForOperator(intersectionMBRAssignOperator);
        intersectionMBRAssignOperator.recomputeSchema();
        MutableObject<ILogicalOperator> intersectionMBRAssignOperatorRef =
                new MutableObject<>(intersectionMBRAssignOperator);

        // Replicate the union MBR to left and right nested loop join(NLJ) operator, and another NLJ for reference point test
        ReplicateOperator intersectionMBRReplicateOperator =
                createReplicateOperator(intersectionMBRAssignOperatorRef, context, op.getSourceLocation(), 3);

        // Replicate union MBR to the left branch
        ExchangeOperator exchMBRToJoinOpLeft =
                createBroadcastExchangeOp(intersectionMBRReplicateOperator, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> exchMBRToJoinOpLeftRef = new MutableObject<>(exchMBRToJoinOpLeft);
        Pair<LogicalVariable, Mutable<ILogicalOperator>> createLeftAssignProjectOperatorResult =
                createAssignProjectOperator(op, intersectionMBR, intersectionMBRReplicateOperator,
                        exchMBRToJoinOpLeftRef, context);
        LogicalVariable leftIntersectionMBRVar = createLeftAssignProjectOperatorResult.getFirst();
        Mutable<ILogicalOperator> leftIntersectionMBRRef = createLeftAssignProjectOperatorResult.getSecond();

        // Replicate union MBR to the right branch
        ExchangeOperator exchMBRToJoinOpRight =
                createBroadcastExchangeOp(intersectionMBRReplicateOperator, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> exchMBRToJoinOpRightRef = new MutableObject<>(exchMBRToJoinOpRight);
        Pair<LogicalVariable, Mutable<ILogicalOperator>> createRightAssignProjectOperatorResult =
                createAssignProjectOperator(op, intersectionMBR, intersectionMBRReplicateOperator,
                        exchMBRToJoinOpRightRef, context);
        LogicalVariable rightIntersectionMBRVar = createRightAssignProjectOperatorResult.getFirst();
        Mutable<ILogicalOperator> rightIntersectionMBRRef = createRightAssignProjectOperatorResult.getSecond();

        // Replicate union MBR to the right branch of a later Nested Loop Join reference point test
        ExchangeOperator exchMBRToReferencePointTestJoinOp =
                createBroadcastExchangeOp(intersectionMBRReplicateOperator, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> exchMBRToReferencePointTestJoinOpRef =
                new MutableObject<>(exchMBRToReferencePointTestJoinOp);

        // Add left Join (TRUE)
        Mutable<ILogicalExpression> leftTrueCondition =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)));
        InnerJoinOperator leftJoinOp =
                new InnerJoinOperator(leftTrueCondition, leftExchToJoinOpRef, leftIntersectionMBRRef);
        leftJoinOp.setSourceLocation(op.getSourceLocation());
        leftJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER,
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        MutableObject<ILogicalOperator> leftJoinRef = new MutableObject<>(leftJoinOp);
        leftJoinOp.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(leftJoinOp);
        leftInputOp.setValue(leftJoinRef.getValue());

        // Add right Join (TRUE)
        Mutable<ILogicalExpression> rightTrueCondition =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)));
        InnerJoinOperator rightJoinOp =
                new InnerJoinOperator(rightTrueCondition, rightExchToJoinOpRef, rightIntersectionMBRRef);
        rightJoinOp.setSourceLocation(op.getSourceLocation());
        rightJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER,
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        MutableObject<ILogicalOperator> rightJoinRef = new MutableObject<>(rightJoinOp);
        rightJoinOp.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(rightJoinOp);
        rightInputOp.setValue(rightJoinRef.getValue());

        Mutable<ILogicalExpression> leftIntersectionMBRExpr =
                new MutableObject<>(new VariableReferenceExpression(leftIntersectionMBRVar));
        Mutable<ILogicalExpression> rightIntersectionMBRExpr =
                new MutableObject<>(new VariableReferenceExpression(rightIntersectionMBRVar));
        Mutable<ILogicalExpression> referencePointTestMBRExpr =
                new MutableObject<>(new VariableReferenceExpression(intersectionMBR));

        // Inject unnest operator to add tile ID to the left and right branch of the join operator
        LogicalVariable leftTileIdVar = SpatialJoinUtils.injectSpatialTileUnnestOperator(context, leftInputOp,
                leftInputVar, leftIntersectionMBRExpr, DEFAULT_ROWS, DEFAULT_COLUMNS);
        LogicalVariable rightTileIdVar = SpatialJoinUtils.injectSpatialTileUnnestOperator(context, rightInputOp,
                rightInputVar, rightIntersectionMBRExpr, DEFAULT_ROWS, DEFAULT_COLUMNS);

        // Reference point test condition will be used as the condition of a Nested Loop Join operator after the
        // spatial join operator. This design allow us to use the union MBR (or summary of the join) efficiently,
        // instead of propagate this variable via Hyracks context or data flow.
        ScalarFunctionCallExpression referenceIdEquiJoinCondition =
                createReferencePointTestCondition(op, referencePointTestMBRExpr, leftTileIdVar, rightTileIdVar,
                        leftInputVar, rightInputVar, DEFAULT_ROWS, DEFAULT_COLUMNS);

        conditionExprs.add(new MutableObject<>(spatialJoinFuncExpr));

        ScalarFunctionCallExpression updatedJoinCondition;
        if (conditionExprs.size() > 1) {
            updatedJoinCondition = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND), conditionExprs);
            updatedJoinCondition.setSourceLocation(op.getSourceLocation());
        } else {
            updatedJoinCondition = (ScalarFunctionCallExpression) spatialJoinFuncExpr;
        }
        Mutable<ILogicalExpression> joinConditionRef = op.getCondition();
        joinConditionRef.setValue(updatedJoinCondition);

        List<LogicalVariable> keysLeftBranch = new ArrayList<>();
        keysLeftBranch.add(leftTileIdVar);
        keysLeftBranch.add(leftInputVar);

        List<LogicalVariable> keysRightBranch = new ArrayList<>();
        keysRightBranch.add(rightTileIdVar);
        keysRightBranch.add(rightInputVar);

        InnerJoinOperator spatialJoinOp =
                new InnerJoinOperator(new MutableObject<>(updatedJoinCondition), leftInputOp, rightInputOp);
        spatialJoinOp.setSourceLocation(op.getSourceLocation());
        SpatialJoinUtils.setSpatialJoinOp(spatialJoinOp, keysLeftBranch, keysRightBranch, context);
        spatialJoinOp.setSchema(op.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(spatialJoinOp);

        Mutable<ILogicalOperator> opRef = new MutableObject<>(op);
        Mutable<ILogicalOperator> spatialJoinOpRef = new MutableObject<>(spatialJoinOp);

        InnerJoinOperator referencePointTestJoinOp =
                new InnerJoinOperator(new MutableObject<>(referenceIdEquiJoinCondition), spatialJoinOpRef,
                        exchMBRToReferencePointTestJoinOpRef);
        referencePointTestJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(
                AbstractBinaryJoinOperator.JoinKind.INNER, AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        MutableObject<ILogicalOperator> referencePointTestJoinOpRef = new MutableObject<>(referencePointTestJoinOp);
        referencePointTestJoinOp.setSourceLocation(op.getSourceLocation());
        context.computeAndSetTypeEnvironmentForOperator(referencePointTestJoinOp);
        referencePointTestJoinOp.recomputeSchema();
        opRef.setValue(referencePointTestJoinOpRef.getValue());
        op.getInputs().clear();
        op.getInputs().addAll(referencePointTestJoinOp.getInputs());
        op.setPhysicalOperator(referencePointTestJoinOp.getPhysicalOperator());
        op.getCondition().setValue(referencePointTestJoinOp.getCondition().getValue());
        context.computeAndSetTypeEnvironmentForOperator(op);
        op.recomputeSchema();
    }

    private static ScalarFunctionCallExpression createReferencePointTestCondition(AbstractBinaryJoinOperator op,
            Mutable<ILogicalExpression> referencePointTestMBRExpr, LogicalVariable leftTileIdVar,
            LogicalVariable rightTileIdVar, LogicalVariable leftInputVar, LogicalVariable rightInputVar, int numRows,
            int numColumns) {
        // Compute reference tile ID
        ScalarFunctionCallExpression referenceTileId = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.REFERENCE_TILE),
                new MutableObject<>(new VariableReferenceExpression(leftInputVar)),
                new MutableObject<>(new VariableReferenceExpression(rightInputVar)), referencePointTestMBRExpr,
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(numRows)))),
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(numColumns)))),
                new MutableObject<>(new VariableReferenceExpression(rightTileIdVar)));
        referenceTileId.setSourceLocation(op.getSourceLocation());

        ScalarFunctionCallExpression referenceIdEquiJoinCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.EQ),
                        new MutableObject<>(new VariableReferenceExpression(leftTileIdVar)),
                        new MutableObject<>(referenceTileId));
        referenceIdEquiJoinCondition.setSourceLocation(op.getSourceLocation());

        return referenceIdEquiJoinCondition;
    }

    private static Pair<LogicalVariable, Mutable<ILogicalOperator>> createAssignProjectOperator(
            AbstractBinaryJoinOperator op, LogicalVariable inputVar, ReplicateOperator replicateOperator,
            MutableObject<ILogicalOperator> exchMBRToForwardRef, IOptimizationContext context)
            throws AlgebricksException {
        LogicalVariable newFinalMbrVar = context.newVar();
        List<LogicalVariable> finalMBRLiveVars = new ArrayList<>();
        finalMBRLiveVars.add(newFinalMbrVar);
        ListSet<LogicalVariable> finalMBRLiveVarsSet = new ListSet<>();
        finalMBRLiveVarsSet.add(newFinalMbrVar);

        Mutable<ILogicalExpression> finalMBRExpr = new MutableObject<>(new VariableReferenceExpression(inputVar));
        AbstractLogicalOperator assignOperator = new AssignOperator(newFinalMbrVar, finalMBRExpr);
        assignOperator.setSourceLocation(op.getSourceLocation());
        assignOperator.setExecutionMode(replicateOperator.getExecutionMode());
        assignOperator.setPhysicalOperator(new AssignPOperator());
        AbstractLogicalOperator projectOperator = new ProjectOperator(finalMBRLiveVars);
        projectOperator.setSourceLocation(op.getSourceLocation());
        projectOperator.setPhysicalOperator(new StreamProjectPOperator());
        projectOperator.setExecutionMode(replicateOperator.getExecutionMode());
        assignOperator.getInputs().add(exchMBRToForwardRef);
        projectOperator.getInputs().add(new MutableObject<ILogicalOperator>(assignOperator));

        context.computeAndSetTypeEnvironmentForOperator(assignOperator);
        assignOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(projectOperator);
        projectOperator.recomputeSchema();
        Mutable<ILogicalOperator> projectOperatorRef = new MutableObject<>(projectOperator);

        return new Pair<>(newFinalMbrVar, projectOperatorRef);
    }

    private static ReplicateOperator createReplicateOperator(Mutable<ILogicalOperator> inputOperator,
            IOptimizationContext context, SourceLocation sourceLocation, int outputArity) throws AlgebricksException {
        ReplicateOperator replicateOperator = new ReplicateOperator(outputArity);
        replicateOperator.setPhysicalOperator(new ReplicatePOperator());
        replicateOperator.setSourceLocation(sourceLocation);
        replicateOperator.getInputs().add(new MutableObject<>(inputOperator.getValue()));
        OperatorManipulationUtil.setOperatorMode(replicateOperator);
        replicateOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(replicateOperator);
        return replicateOperator;
    }

    private static ExchangeOperator createOneToOneExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setSourceLocation(sourceLocation);
        exchangeOperator.setPhysicalOperator(new OneToOneExchangePOperator());
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static ExchangeOperator createRandomPartitionExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setSourceLocation(sourceLocation);
        exchangeOperator.setPhysicalOperator(new RandomPartitionExchangePOperator(context.getComputationNodeDomain()));
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static ExchangeOperator createBroadcastExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setSourceLocation(sourceLocation);
        exchangeOperator.setPhysicalOperator(new BroadcastExchangePOperator(context.getComputationNodeDomain()));
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createLocalAndGlobalAggregateOperators(
            AbstractBinaryJoinOperator op, IOptimizationContext context, LogicalVariable inputVar,
            MutableObject<ILogicalOperator> exchToLocalAggRef) throws AlgebricksException {
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
        fields.add(new MutableObject<>(inputVarRef));

        // Create local aggregate operator
        IFunctionInfo localAggFunc = context.getMetadataProvider().lookupFunction(BuiltinFunctions.LOCAL_UNION_MBR);
        AggregateFunctionCallExpression localAggExpr = new AggregateFunctionCallExpression(localAggFunc, false, fields);
        localAggExpr.setSourceLocation(op.getSourceLocation());
        localAggExpr.setOpaqueParameters(new Object[] {});
        List<LogicalVariable> localAggResultVars = new ArrayList<>(1);
        List<Mutable<ILogicalExpression>> localAggFuncs = new ArrayList<>(1);
        LogicalVariable localOutVariable = context.newVar();
        localAggResultVars.add(localOutVariable);
        localAggFuncs.add(new MutableObject<>(localAggExpr));
        AggregateOperator localAggOperator = createAggregate(localAggResultVars, false, localAggFuncs,
                exchToLocalAggRef, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> localAgg = new MutableObject<>(localAggOperator);

        // Output of local aggregate operator is the input of global aggregate operator
        return createGlobalAggregateOperator(op, context, localOutVariable, localAgg);
    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createGlobalAggregateOperator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, LogicalVariable inputVar,
            MutableObject<ILogicalOperator> inputOperator) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> globalAggFuncArgs = new ArrayList<>(1);
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        globalAggFuncArgs.add(new MutableObject<>(inputVarRef));
        IFunctionInfo globalAggFunc = context.getMetadataProvider().lookupFunction(BuiltinFunctions.GLOBAL_UNION_MBR);
        AggregateFunctionCallExpression globalAggExpr =
                new AggregateFunctionCallExpression(globalAggFunc, true, globalAggFuncArgs);
        globalAggExpr.setStepOneAggregate(globalAggFunc);
        globalAggExpr.setStepTwoAggregate(globalAggFunc);
        globalAggExpr.setSourceLocation(op.getSourceLocation());
        globalAggExpr.setOpaqueParameters(new Object[] {});
        List<LogicalVariable> globalAggResultVars = new ArrayList<>(1);
        globalAggResultVars.add(context.newVar());
        List<Mutable<ILogicalExpression>> globalAggFuncs = new ArrayList<>(1);
        globalAggFuncs.add(new MutableObject<>(globalAggExpr));
        AggregateOperator globalAggOperator = createAggregate(globalAggResultVars, true, globalAggFuncs, inputOperator,
                context, op.getSourceLocation());
        globalAggOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(globalAggOperator);
        MutableObject<ILogicalOperator> globalAgg = new MutableObject<>(globalAggOperator);
        return new Pair<>(globalAgg, globalAggResultVars);
    }

    private static Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> createDynamicMBRCalculator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, Mutable<ILogicalOperator> inputOp,
            LogicalVariable inputVar) throws AlgebricksException {
        // Add ReplicationOperator for the input branch
        SourceLocation sourceLocation = op.getSourceLocation();
        ReplicateOperator replicateOperator = createReplicateOperator(inputOp, context, sourceLocation, 2);

        // Create one to one exchange operators for the replicator of the input branch
        ExchangeOperator exchToForward = createRandomPartitionExchangeOp(replicateOperator, context, sourceLocation);
        MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchToForward);

        ExchangeOperator exchToLocalAgg = createOneToOneExchangeOp(replicateOperator, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> exchToLocalAggRef = new MutableObject<>(exchToLocalAgg);

        // Materialize the data to be able to re-read the data again
        replicateOperator.getOutputMaterializationFlags()[0] = true;

        Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createLocalAndGlobalAggResult =
                createLocalAndGlobalAggregateOperators(op, context, inputVar, exchToLocalAggRef);
        return new Triple<>(createLocalAndGlobalAggResult.first, createLocalAndGlobalAggResult.second,
                exchToForwardRef);
    }

    /**
     * Creates an aggregate operator. $$resultVariables = expressions()
     * @param resultVariables the variables which stores the result of the aggregation
     * @param isGlobal whether the aggregate operator is a global or local one
     * @param expressions the aggregation functions desired
     * @param inputOperator the input op that is feeding the aggregate operator
     * @param context optimization context
     * @param sourceLocation source location
     * @return an aggregate operator with the specified information
     * @throws AlgebricksException when there is error setting the type environment of the newly created aggregate op
     */
    private static AggregateOperator createAggregate(List<LogicalVariable> resultVariables, boolean isGlobal,
            List<Mutable<ILogicalExpression>> expressions, MutableObject<ILogicalOperator> inputOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        AggregateOperator aggregateOperator = new AggregateOperator(resultVariables, expressions);
        aggregateOperator.setPhysicalOperator(new AggregatePOperator());
        aggregateOperator.setSourceLocation(sourceLocation);
        aggregateOperator.getInputs().add(inputOperator);
        aggregateOperator.setGlobal(isGlobal);
        if (!isGlobal) {
            aggregateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
        } else {
            aggregateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        }
        aggregateOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(aggregateOperator);
        return aggregateOperator;
    }

    private static Mutable<ILogicalExpression> createRectangleExpression(SpatialJoinAnnotation spatialJoinAnn) {
        return new MutableObject<>(new ConstantExpression(
                new AsterixConstantValue(new ARectangle(new APoint(spatialJoinAnn.getMinX(), spatialJoinAnn.getMinY()),
                        new APoint(spatialJoinAnn.getMaxX(), spatialJoinAnn.getMaxY())))));
    }
}
