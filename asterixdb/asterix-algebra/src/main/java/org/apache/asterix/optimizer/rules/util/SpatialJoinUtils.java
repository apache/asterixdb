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
import java.util.UUID;

import org.apache.asterix.algebra.operators.physical.SpatialJoinPOperator;
import org.apache.asterix.common.annotations.SpatialJoinAnnotation;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.BroadcastExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SpatialForwardPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.rewriter.rules.EnforceStructuralPropertiesRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class SpatialJoinUtils {

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

        // We only apply optimization process for spatial join if the join annotation (hint) is provided
        SpatialJoinAnnotation spatialJoinAnn = spatialJoinFuncExpr.getAnnotation(SpatialJoinAnnotation.class);
        if (spatialJoinAnn != null) {
            SpatialJoinUtils.updateJoinPlan(op, spatialJoinFuncExpr, conditionExprs, spatialJoinAnn, context, left,
                    right);
            return true;
        } else {
            return false;
        }
    }

    private static void setSpatialJoinOp(AbstractBinaryJoinOperator op, List<LogicalVariable> keysLeftBranch,
            List<LogicalVariable> keysRightBranch, IOptimizationContext context) {
        ISpatialJoinUtilFactory isjuf = new IntersectSpatialJoinUtilFactory();
        op.setPhysicalOperator(new SpatialJoinPOperator(op.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.PAIRWISE, keysLeftBranch, keysRightBranch,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), isjuf));
    }

    private static LogicalVariable injectSpatialTileUnnestOperator(IOptimizationContext context,
            Mutable<ILogicalOperator> sideOp, LogicalVariable inputVar, Mutable<ILogicalExpression> unionMBRExpr,
            SpatialJoinAnnotation spatialJoinAnn) throws AlgebricksException {
        SourceLocation srcLoc = sideOp.getValue().getSourceLocation();
        LogicalVariable sideVar = context.newVar();
        VariableReferenceExpression inputVarRef = new VariableReferenceExpression(inputVar);
        inputVarRef.setSourceLocation(srcLoc);
        UnnestingFunctionCallExpression funcExpr = new UnnestingFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_TILE),
                new MutableObject<>(inputVarRef), unionMBRExpr,
                new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new AInt64(spatialJoinAnn.getNumRows())))),
                new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new AInt64(spatialJoinAnn.getNumColumns())))));
        funcExpr.setSourceLocation(srcLoc);
        UnnestOperator sideUnnestOp = new UnnestOperator(sideVar, new MutableObject<>(funcExpr));
        sideUnnestOp.setSchema(sideOp.getValue().getSchema());
        sideUnnestOp.setSourceLocation(srcLoc);
        sideUnnestOp.getInputs().add(new MutableObject<>(sideOp.getValue()));
        sideOp.setValue(sideUnnestOp);
        context.computeAndSetTypeEnvironmentForOperator(sideUnnestOp);

        return sideVar;
    }

    private static LogicalVariable injectSpatialAttachAssignOperator(IOptimizationContext context,
            Mutable<ILogicalOperator> sideOp, LogicalVariable inputVar, String mbrKey) throws AlgebricksException {
        SourceLocation srcLoc = sideOp.getValue().getSourceLocation();
        LogicalVariable sideVar = context.newVar();
        VariableReferenceExpression sideInputVar = new VariableReferenceExpression(inputVar);
        sideInputVar.setSourceLocation(srcLoc);
        ScalarFunctionCallExpression funcExpr = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_ATTACH),
                new MutableObject<>(sideInputVar),
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AString(mbrKey)))));
        funcExpr.setSourceLocation(srcLoc);
        AssignOperator sideAssignOp = new AssignOperator(sideVar, new MutableObject<>(funcExpr));
        sideAssignOp.setSchema(sideOp.getValue().getSchema());
        sideAssignOp.setSourceLocation(srcLoc);
        sideAssignOp.getInputs().add(new MutableObject<>(sideOp.getValue()));
        sideOp.setValue(sideAssignOp);
        context.computeAndSetTypeEnvironmentForOperator(sideAssignOp);

        return sideVar;
    }

    protected static void updateJoinPlan(AbstractBinaryJoinOperator op,
            AbstractFunctionCallExpression spatialJoinFuncExpr, List<Mutable<ILogicalExpression>> conditionExprs,
            SpatialJoinAnnotation spatialJoinAnn, IOptimizationContext context, int LEFT, int RIGHT)
            throws AlgebricksException {
        // Extracts spatial intersect function's arguments
        List<Mutable<ILogicalExpression>> spatialJoinArgs = spatialJoinFuncExpr.getArguments();
        if (spatialJoinArgs.size() != 2) {
            return;
        }

        ILogicalExpression spatialJoinLeftArg = spatialJoinArgs.get(LEFT).getValue();
        ILogicalExpression spatialJoinRightArg = spatialJoinArgs.get(RIGHT).getValue();

        // Left and right arguments of the spatial_intersect function should be either variable or function call.
        if (spatialJoinLeftArg.getExpressionTag() == LogicalExpressionTag.CONSTANT
                || spatialJoinRightArg.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return;
        }

        // Gets both input branches of the spatial join.
        Mutable<ILogicalOperator> leftInputOp = op.getInputs().get(LEFT);
        Mutable<ILogicalOperator> rightInputOp = op.getInputs().get(RIGHT);

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

        boolean useDynamicMBR = (spatialJoinAnn.getMinX() == 0.0) && (spatialJoinAnn.getMinY() == 0.0)
                && (spatialJoinAnn.getMaxX() == 0.0) && (spatialJoinAnn.getMaxY() == 0.0);

        String leftAggKey = "";
        String rightAggKey = "";
        LogicalVariable leftUnionMBRVar = null;
        LogicalVariable rightUnionMBRVar = null;
        LogicalVariable finalMBR = null;
        MutableObject<ILogicalOperator> exchMBRToReferencePointTestRef = null;

        if (useDynamicMBR) {
            // Add a dynamic workflow to compute MBR of the left branch
            Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> leftMBRCalculator =
                    createDynamicMBRCalculator(op, context, leftInputOp, leftInputVar);
            MutableObject<ILogicalOperator> leftGlobalAgg = leftMBRCalculator.first;
            List<LogicalVariable> leftGlobalAggResultVars = leftMBRCalculator.second;
            MutableObject<ILogicalOperator> leftExchToForwardRef = leftMBRCalculator.third;
            LogicalVariable leftMBRVar = leftGlobalAggResultVars.get(0);

            // Add a dynamic workflow to compute MBR of the right branch
            Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> rightMBRCalculator =
                    createDynamicMBRCalculator(op, context, rightInputOp, rightInputVar);
            MutableObject<ILogicalOperator> rightGlobalAgg = rightMBRCalculator.first;
            List<LogicalVariable> rightGlobalAggResultVars = rightMBRCalculator.second;
            MutableObject<ILogicalOperator> rightExchToForwardRef = rightMBRCalculator.third;
            LogicalVariable rightMBRVar = rightGlobalAggResultVars.get(0);

            // TODO: investigate the idea of using assign operator instead of union and aggregate operator.
            //  This could reduce the partitioning space, thus it might reduce the partitioning time.

            // Union the results of left and right aggregators
            LogicalVariable unionMBRVar = context.newVar();
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> unionVarMap =
                    new Triple<>(leftMBRVar, rightMBRVar, unionMBRVar);
            List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> unionVarMaps = new ArrayList<>();
            unionVarMaps.add(unionVarMap);
            UnionAllOperator unionAllOperator = new UnionAllOperator(unionVarMaps);
            unionAllOperator.setSourceLocation(op.getSourceLocation());
            unionAllOperator.getInputs().add(new MutableObject<>(leftGlobalAgg.getValue()));
            unionAllOperator.getInputs().add(new MutableObject<>(rightGlobalAgg.getValue()));
            OperatorManipulationUtil.setOperatorMode(unionAllOperator);
            unionAllOperator.recomputeSchema();
            context.computeAndSetTypeEnvironmentForOperator(unionAllOperator);
            MutableObject<ILogicalOperator> unionAllOperatorRef = new MutableObject<>(unionAllOperator);

            // Compute the union MBR of the left and the right MBR
            Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> globalAggregateOperator =
                    createGlobalAggregateOperator(op, context, unionMBRVar, unionAllOperatorRef);
            MutableObject<ILogicalOperator> globalAgg = globalAggregateOperator.first;
            finalMBR = globalAggregateOperator.second.get(0);

            // Replicate the union MBR to left and right forward operator
            ReplicateOperator unionMBRReplicateOperator =
                    createReplicateOperator(globalAgg, context, op.getSourceLocation(), 3);
            ExchangeOperator exchMBRToForwardLeft = createBroadcastExchangeOp(unionMBRReplicateOperator, context);
            MutableObject<ILogicalOperator> exchMBRToForwardLeftRef = new MutableObject<>(exchMBRToForwardLeft);
            ExchangeOperator exchMBRToForwardRight = createBroadcastExchangeOp(unionMBRReplicateOperator, context);
            MutableObject<ILogicalOperator> exchMBRToForwardRightRef = new MutableObject<>(exchMBRToForwardRight);
            ExchangeOperator exchMBRToReferencePointTest = createBroadcastExchangeOp(unionMBRReplicateOperator, context);
            exchMBRToReferencePointTestRef = new MutableObject<>(exchMBRToReferencePointTest);

//            unionMBRReplicateOperator.getOutputMaterializationFlags()[0] = true;
//            unionMBRReplicateOperator.getOutputMaterializationFlags()[1] = true;
//            unionMBRReplicateOperator.getOutputMaterializationFlags()[2] = true;

            Pair<LogicalVariable, Mutable<ILogicalOperator>> createLeftAssignProjectOperatorResult = createAssignProjectOperator(op, finalMBR, unionMBRReplicateOperator, exchMBRToForwardLeftRef, context);
            LogicalVariable leftFinalMBR = createLeftAssignProjectOperatorResult.getFirst();
            Mutable<ILogicalOperator> leftProjectOperatorRef = createLeftAssignProjectOperatorResult.getSecond();

            Pair<LogicalVariable, Mutable<ILogicalOperator>> createRightAssignProjectOperatorResult = createAssignProjectOperator(op, finalMBR, unionMBRReplicateOperator, exchMBRToForwardRightRef, context);
            LogicalVariable rightFinalMBR = createRightAssignProjectOperatorResult.getFirst();
            Mutable<ILogicalOperator> rightProjectOperatorRef = createRightAssignProjectOperatorResult.getSecond();
//
//            Pair<LogicalVariable, Mutable<ILogicalOperator>> createReferenceAssignProjectOperatorResult = createAssignProjectOperator(op, finalMBR, unionMBRReplicateOperator, exchMBRToForwardLeftRef, context);
//            LogicalVariable rightFinalMBR = createReferenceAssignProjectOperatorResult.getFirst();
//            Mutable<ILogicalOperator> rightProjectOperatorRef = createReferenceAssignProjectOperatorResult.getSecond();

            // Add left Join (TRUE)
            Mutable<ILogicalExpression> leftTrueCondition = new MutableObject<>(
                new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)));
            InnerJoinOperator leftJoinOp = new InnerJoinOperator(leftTrueCondition, leftExchToForwardRef, leftProjectOperatorRef);
            leftJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER, AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
            MutableObject<ILogicalOperator> leftJoinRef = new MutableObject<>(leftJoinOp);
            context.computeAndSetTypeEnvironmentForOperator(leftJoinOp);
            leftInputOp.setValue(leftJoinRef.getValue());

            // Add right Join (TRUE)
            Mutable<ILogicalExpression> rightTrueCondition = new MutableObject<>(
                new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)));
            InnerJoinOperator rightJoinOp = new InnerJoinOperator(rightTrueCondition, rightExchToForwardRef, rightProjectOperatorRef);
            rightJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER, AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
            MutableObject<ILogicalOperator> rightJoinRef = new MutableObject<>(rightJoinOp);
            context.computeAndSetTypeEnvironmentForOperator(rightJoinOp);
            rightInputOp.setValue(rightJoinRef.getValue());

            leftUnionMBRVar = leftFinalMBR;
            rightUnionMBRVar = rightFinalMBR;

            /*
            // Add forward operator to the left branch
            leftAggKey = UUID.randomUUID().toString();
            ForwardOperator leftForward = createForward(leftAggKey, finalMBR, leftExchToForwardRef,
                    exchMBRToForwardLeftRef, context, op.getSourceLocation());
            MutableObject<ILogicalOperator> leftForwardRef = new MutableObject<>(leftForward);
            leftInputOp.setValue(leftForwardRef.getValue());

            // Add forward operator to the right branch
            rightAggKey = UUID.randomUUID().toString();
            ForwardOperator rightForward = createForward(rightAggKey, finalMBR, rightExchToForwardRef,
                    exchMBRToForwardRightRef, context, op.getSourceLocation());
            MutableObject<ILogicalOperator> rightForwardRef = new MutableObject<>(rightForward);
            rightInputOp.setValue(rightForwardRef.getValue());

            // Inject assign operator to add the union MBR to the left and right branch of the join operator
            leftUnionMBRVar =
                    SpatialJoinUtils.injectSpatialAttachAssignOperator(context, leftInputOp, leftInputVar, leftAggKey);
            rightUnionMBRVar = SpatialJoinUtils.injectSpatialAttachAssignOperator(context, rightInputOp, rightInputVar,
                    rightAggKey);
            */
        }

        Mutable<ILogicalExpression> leftUnionMBRExpr;
        Mutable<ILogicalExpression> rightUnionMBRExpr;
        Mutable<ILogicalExpression> finalMBRExpr = null;
        if (useDynamicMBR) {
            leftUnionMBRExpr = new MutableObject<>(new VariableReferenceExpression(leftUnionMBRVar));
            rightUnionMBRExpr = new MutableObject<>(new VariableReferenceExpression(rightUnionMBRVar));
            finalMBRExpr = new MutableObject<>(new VariableReferenceExpression(finalMBR));
        } else {
            leftUnionMBRExpr = new MutableObject<>(new ConstantExpression(new AsterixConstantValue(
                    new ARectangle(new APoint(spatialJoinAnn.getMinX(), spatialJoinAnn.getMinY()),
                            new APoint(spatialJoinAnn.getMaxX(), spatialJoinAnn.getMaxY())))));
            rightUnionMBRExpr = new MutableObject<>(new ConstantExpression(new AsterixConstantValue(
                    new ARectangle(new APoint(spatialJoinAnn.getMinX(), spatialJoinAnn.getMinY()),
                            new APoint(spatialJoinAnn.getMaxX(), spatialJoinAnn.getMaxY())))));
        }

        // Inject unnest operator to add tile ID to the left and right branch of the join operator
        LogicalVariable leftTileIdVar = SpatialJoinUtils.injectSpatialTileUnnestOperator(context, leftInputOp,
                leftInputVar, leftUnionMBRExpr, spatialJoinAnn);
        LogicalVariable rightTileIdVar = SpatialJoinUtils.injectSpatialTileUnnestOperator(context, rightInputOp,
                rightInputVar, rightUnionMBRExpr, spatialJoinAnn);

        // Compute reference tile ID
        ScalarFunctionCallExpression referenceTileId = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.REFERENCE_TILE),
                new MutableObject<>(new VariableReferenceExpression(leftInputVar)),
                new MutableObject<>(new VariableReferenceExpression(rightInputVar)), finalMBRExpr,
                new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new AInt64(spatialJoinAnn.getNumRows())))),
                new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new AInt64(spatialJoinAnn.getNumColumns())))));

        // Update the join conditions with the tile Id equality condition
        ScalarFunctionCallExpression tileIdEquiJoinCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.EQ),
                        new MutableObject<>(new VariableReferenceExpression(leftTileIdVar)),
                        new MutableObject<>(new VariableReferenceExpression(rightTileIdVar)));
        ScalarFunctionCallExpression referenceIdEquiJoinCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.EQ),
                        new MutableObject<>(new VariableReferenceExpression(leftTileIdVar)),
                        new MutableObject<>(referenceTileId));

        //        ScalarFunctionCallExpression spatialIntersectCondition1 = new ScalarFunctionCallExpression(
        //                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_INTERSECT),
        //                new MutableObject<>(new VariableReferenceExpression(leftUnionMBRVar)),
        //                new MutableObject<>(new VariableReferenceExpression(rightUnionMBRVar)));
                ScalarFunctionCallExpression spatialIntersectCondition2 = new ScalarFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_INTERSECT),
                        new MutableObject<>(new VariableReferenceExpression(leftInputVar)),
                        new MutableObject<>(new VariableReferenceExpression(rightInputVar)));
        //        conditionExprs.add(new MutableObject<>(spatialIntersectCondition1));
//                conditionExprs.add(new MutableObject<>(spatialIntersectCondition2));
        //        conditionExprs.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE))));

        conditionExprs.add(new MutableObject<>(tileIdEquiJoinCondition));
//        conditionExprs.add(new MutableObject<>(referenceIdEquiJoinCondition));

        ScalarFunctionCallExpression updatedJoinCondition = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND), conditionExprs);
        Mutable<ILogicalExpression> joinConditionRef = op.getCondition();
        joinConditionRef.setValue(updatedJoinCondition);

        List<LogicalVariable> keysLeftBranch = new ArrayList<>();
        keysLeftBranch.add(leftTileIdVar);
        keysLeftBranch.add(leftInputVar);

        List<LogicalVariable> keysRightBranch = new ArrayList<>();
        keysRightBranch.add(rightTileIdVar);
        keysRightBranch.add(rightInputVar);

//        if (useDynamicMBR) {
//            keysLeftBranch.add(leftUnionMBRVar);
//            keysRightBranch.add(rightUnionMBRVar);
//        }

//        SpatialJoinUtils.setSpatialJoinOp(op, keysLeftBranch, keysRightBranch, context);

        InnerJoinOperator spatialJoinOp = new InnerJoinOperator(new MutableObject<>(updatedJoinCondition), leftInputOp, rightInputOp);
        spatialJoinOp.setSourceLocation(op.getSourceLocation());
        SpatialJoinUtils.setSpatialJoinOp(spatialJoinOp, keysLeftBranch, keysRightBranch, context);
        spatialJoinOp.setSchema(op.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(spatialJoinOp);

        Mutable<ILogicalOperator> opRef = new MutableObject<>(op);
        Mutable<ILogicalOperator> spatialJoinOpRef = new MutableObject<>(spatialJoinOp);

//        ReplicateOperator spatialJoinReplicateOperator =
//            createReplicateOperator(spatialJoinOpRef, context, op.getSourceLocation(), 1);
//        ExchangeOperator exchSpatialJoinResult =chị createOneToOneExchangeOp(spatialJoinReplicateOperator, context);
//        MutableObject<ILogicalOperator> exchSpatialJoinResultRef = new MutableObject<>(exchSpatialJoinResult);
//        opRef.setValue(spatialJoinOpRef.getValue());
//        op.setPhysicalOperator(spatialJoinOp.getPhysicalOperator());
//        SpatialJoinUtils.setSpatialJoinOp(op, keysLeftBranch, keysRightBranch, context);
        if (useDynamicMBR) {
            InnerJoinOperator referencePointTestJoinOp = new InnerJoinOperator(new MutableObject<>(referenceIdEquiJoinCondition), spatialJoinOpRef, exchMBRToReferencePointTestRef);
            referencePointTestJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER, AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
            MutableObject<ILogicalOperator> referencePointTestJoinOpRef = new MutableObject<>(referencePointTestJoinOp);
            referencePointTestJoinOp.setSourceLocation(op.getSourceLocation());
            referencePointTestJoinOp.recomputeSchema();
            context.computeAndSetTypeEnvironmentForOperator(referencePointTestJoinOp);
            opRef.setValue(referencePointTestJoinOpRef.getValue());
            op.getInputs().clear();
            op.getInputs().addAll(referencePointTestJoinOp.getInputs());
            op.setSchema(referencePointTestJoinOp.getSchema());
            op.setPhysicalOperator(referencePointTestJoinOp.getPhysicalOperator());
            op.getCondition().setValue(referencePointTestJoinOp.getCondition().getValue());
        }
    }

    private static Pair<LogicalVariable, Mutable<ILogicalOperator>> createAssignProjectOperator(AbstractBinaryJoinOperator op, LogicalVariable finalMBR,
                                                                                      ReplicateOperator unionMBRReplicateOperator, MutableObject<ILogicalOperator> exchMBRToForwardLeftRef, IOptimizationContext context) throws AlgebricksException {
        LogicalVariable newFinalMbrVar = context.newVar();
        List<LogicalVariable> finalMBRLiveVars = new ArrayList<>();
        finalMBRLiveVars.add(newFinalMbrVar);
        ListSet<LogicalVariable> finalMBRLiveVarsSet = new ListSet<>();
        finalMBRLiveVarsSet.add(newFinalMbrVar);

        Mutable<ILogicalExpression> finalMBRExpr = new MutableObject<>(new VariableReferenceExpression(finalMBR));
        AbstractLogicalOperator assignOperator = new AssignOperator(newFinalMbrVar, finalMBRExpr);
        assignOperator.setSourceLocation(op.getSourceLocation());
        assignOperator.setExecutionMode(unionMBRReplicateOperator.getExecutionMode());
        assignOperator.setPhysicalOperator(new AssignPOperator());
        AbstractLogicalOperator projectOperator = new ProjectOperator(finalMBRLiveVars);
        projectOperator.setSourceLocation(op.getSourceLocation());
        projectOperator.setPhysicalOperator(new StreamProjectPOperator());
        projectOperator.setExecutionMode(unionMBRReplicateOperator.getExecutionMode());
        assignOperator.getInputs().add(exchMBRToForwardLeftRef);
        projectOperator.getInputs().add(new MutableObject<ILogicalOperator>(assignOperator));

        // set the types
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
            IOptimizationContext context) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setPhysicalOperator(new OneToOneExchangePOperator());
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static ExchangeOperator createBroadcastExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
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
        //        ConstantExpression one = new ConstantExpression(new AsterixConstantValue(new AInt64(1)));
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
        fields.add(new MutableObject<>(inputVarRef));
        //        fields.add(new MutableObject<>(one));

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
        AggregateOperator localAggOperator = EnforceStructuralPropertiesRule.createAggregate(localAggResultVars, false,
                localAggFuncs, exchToLocalAggRef, context, op.getSourceLocation());
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
        AggregateOperator globalAggOperator = EnforceStructuralPropertiesRule.createAggregate(globalAggResultVars, true,
                globalAggFuncs, inputOperator, context, op.getSourceLocation());
        globalAggOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(globalAggOperator);
        MutableObject<ILogicalOperator> globalAgg = new MutableObject<>(globalAggOperator);
        return new Pair<>(globalAgg, globalAggResultVars);
    }

    private static ForwardOperator createForward(String aggResultKey, LogicalVariable aggResultVariable,
            MutableObject<ILogicalOperator> exchangeOpFromReplicate, MutableObject<ILogicalOperator> globalAggInput,
            IOptimizationContext context, SourceLocation sourceLoc) throws AlgebricksException {
        AbstractLogicalExpression aggResultExpression = new VariableReferenceExpression(aggResultVariable, sourceLoc);
        ForwardOperator forwardOperator = new ForwardOperator(aggResultKey, new MutableObject<>(aggResultExpression));
        forwardOperator.setSourceLocation(sourceLoc);
        forwardOperator.setPhysicalOperator(new SpatialForwardPOperator());
        forwardOperator.getInputs().add(exchangeOpFromReplicate);
        forwardOperator.getInputs().add(globalAggInput);
        OperatorManipulationUtil.setOperatorMode(forwardOperator);
        forwardOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(forwardOperator);
        return forwardOperator;
    }

    private static Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> createDynamicMBRCalculator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, Mutable<ILogicalOperator> inputOp,
            LogicalVariable inputVar) throws AlgebricksException {
        // Add ReplicationOperator for the input branch
        SourceLocation sourceLocation = op.getSourceLocation();
        ReplicateOperator replicateOperator = createReplicateOperator(inputOp, context, sourceLocation, 2);

        // Create one to one exchange operators for the replicator of the input branch
        ExchangeOperator exchToForward = createOneToOneExchangeOp(replicateOperator, context);
        MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchToForward);

        ExchangeOperator exchToLocalAgg = createOneToOneExchangeOp(replicateOperator, context);
        MutableObject<ILogicalOperator> exchToLocalAggRef = new MutableObject<>(exchToLocalAgg);

        // Materialize the data to be able to re-read the data again
        replicateOperator.getOutputMaterializationFlags()[0] = true;

        Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createLocalAndGlobalAggResult =
                createLocalAndGlobalAggregateOperators(op, context, inputVar, exchToLocalAggRef);
        return new Triple<>(createLocalAndGlobalAggResult.first, createLocalAndGlobalAggResult.second,
                exchToForwardRef);
    }
}
