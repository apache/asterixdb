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
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.runtime.operators.joins.spatial.utils.ISpatialJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.spatial.utils.IntersectSpatialJoinUtilFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.BroadcastExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SpatialForwardPOperator;
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
            Mutable<ILogicalOperator> sideOp, LogicalVariable inputVar, SpatialJoinAnnotation spatialJoinAnn)
            throws AlgebricksException {
        SourceLocation srcLoc = sideOp.getValue().getSourceLocation();
        LogicalVariable sideVar = context.newVar();
        VariableReferenceExpression sideInputVar = new VariableReferenceExpression(inputVar);
        sideInputVar.setSourceLocation(srcLoc);
        UnnestingFunctionCallExpression funcExpr = new UnnestingFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_TILE),
                new MutableObject<>(sideInputVar),
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(
                        new ARectangle(new APoint(spatialJoinAnn.getMinX(), spatialJoinAnn.getMinY()),
                                new APoint(spatialJoinAnn.getMaxX(), spatialJoinAnn.getMaxY()))))),
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

        // Add ReplicationOperator
        ReplicateOperator replicateOperator = new ReplicateOperator(2);
        replicateOperator.setSourceLocation(op.getSourceLocation());
        replicateOperator.setPhysicalOperator(new ReplicatePOperator());
        replicateOperator.setSchema(leftInputOp.getValue().getSchema());
        replicateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        replicateOperator.getInputs().add(new MutableObject<>(leftInputOp.getValue()));
        context.computeAndSetTypeEnvironmentForOperator(replicateOperator);

//        replicateOperator.getOutputMaterializationFlags()[1] = true;

//        leftInputOp.setValue(replicateOperator);

        // Add one-to-one exchange
        ExchangeOperator exchangeOperator1 = new ExchangeOperator();
        exchangeOperator1.setPhysicalOperator(new OneToOneExchangePOperator());
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator1));
        exchangeOperator1.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator1.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator1.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator1);
        MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchangeOperator1);

        ExchangeOperator exchangeOperator2 = new ExchangeOperator();
        exchangeOperator2.setPhysicalOperator(new OneToOneExchangePOperator());
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator2));
        exchangeOperator2.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator2.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator2.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator2);
        MutableObject<ILogicalOperator> exchToAggRef = new MutableObject<>(exchangeOperator2);

        replicateOperator.getOutputMaterializationFlags()[0] = true;
//        replicateOperator.getOutputMaterializationFlags()[1] = true;

        // Add agg operator
        ConstantExpression one = new ConstantExpression(new AsterixConstantValue(new AInt64(1)));
        AbstractLogicalExpression leftInputVarRef = new VariableReferenceExpression(leftInputVar, op.getSourceLocation());
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
        fields.add(new MutableObject<>(one));
//        fields.add(new MutableObject<>(leftInputVarRef));
//        for (LogicalVariable var: liveVars) {
//            AbstractLogicalExpression varRef = new VariableReferenceExpression(var, op.getSourceLocation());
//            fields.add(new MutableObject<>(varRef));
//        }

        // Local function
        IFunctionInfo countFunc = context.getMetadataProvider().lookupFunction(BuiltinFunctions.SQL_COUNT);
        AggregateFunctionCallExpression countExpr = new AggregateFunctionCallExpression(countFunc, false, fields);
        countExpr.setSourceLocation(op.getSourceLocation());
        countExpr.setOpaqueParameters(new Object[] {});

        List<LogicalVariable> aggResultVar = new ArrayList<>(1);
        List<Mutable<ILogicalExpression>> aggFunc = new ArrayList<>(1);
        LogicalVariable localOutVariable = context.newVar();
        aggResultVar.add(localOutVariable);
        aggFunc.add(new MutableObject<>(countExpr));

        AggregateOperator aggOp = EnforceStructuralPropertiesRule.createAggregate(aggResultVar, false, aggFunc, exchToAggRef, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> localAgg = new MutableObject<>(aggOp);

        // Global function
        List<Mutable<ILogicalExpression>> argsToGlobal = new ArrayList<>(1);
        AbstractLogicalExpression varExprRef = new VariableReferenceExpression(localOutVariable, op.getSourceLocation());
        argsToGlobal.add(new MutableObject<>(varExprRef));
        IFunctionInfo globalFunc = context.getMetadataProvider().lookupFunction(BuiltinFunctions.SQL_SUM);
        AggregateFunctionCallExpression globalExp = new AggregateFunctionCallExpression(globalFunc, true, argsToGlobal);
        globalExp.setStepOneAggregate(globalFunc);
        globalExp.setStepTwoAggregate(globalFunc);
        globalExp.setSourceLocation(op.getSourceLocation());
        globalExp.setOpaqueParameters(new Object[] {});
        List<LogicalVariable> globalResultVariable = new ArrayList<>(1);
        List<Mutable<ILogicalExpression>> globalAggFunction = new ArrayList<>(1);
        globalResultVariable.add(context.newVar());
        globalAggFunction.add(new MutableObject<>(globalExp));

        AggregateOperator globalAggOp = EnforceStructuralPropertiesRule.createAggregate(globalResultVariable, true, globalAggFunction, localAgg, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> globalAgg = new MutableObject<>(globalAggOp);


        // Add forward operator
        String aggKey = UUID.randomUUID().toString();
        LogicalVariable aggVar = globalResultVariable.get(0);
//        ForwardOperator forward = createForward(aggKey, null, exchToForwardRef, null, context, srcLoc);
        ForwardOperator forward = createForward(aggKey, aggVar, exchToForwardRef, globalAgg, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> forwardRef = new MutableObject<>(forward);

        leftInputOp.setValue(forwardRef.getValue());
//        rightInputOp.setValue(exchangeOperator2);

        // Inject unnest operator to the left and right branch of the join operator // leftInputOp  new MutableObject<>(replicateOp)
        LogicalVariable leftTileIdVar =
            SpatialJoinUtils.injectSpatialTileUnnestOperator(context, leftInputOp, leftInputVar, spatialJoinAnn);
        LogicalVariable rightTileIdVar =
            SpatialJoinUtils.injectSpatialTileUnnestOperator(context, rightInputOp, rightInputVar, spatialJoinAnn);

//        MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchangeOperator1);
//        replicateOperator.getOutputs().add(exchToForwardRef);
////        context.computeAndSetTypeEnvironmentForOperator(replicateOperator);
//
//        ExchangeOperator exchangeOperator2 = new ExchangeOperator();
//        exchangeOperator2.setPhysicalOperator(new OneToOneExchangePOperator());
//        exchangeOperator2.getInputs().add(new MutableObject<>(replicateOperator));
//        exchangeOperator2.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
//        exchangeOperator2.setSchema(replicateOperator.getSchema());
//        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator2);
//        MutableObject<ILogicalOperator> exchToAggRef = new MutableObject<>(exchangeOperator2);
//        replicateOperator.getOutputs().add(exchToAggRef);

//        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));

        // Add forward operator
        /*SourceLocation srcLoc = op.getSourceLocation();
        AbstractLogicalExpression leftInputVarRef = new VariableReferenceExpression(leftInputVar, srcLoc);
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
        fields.add(new MutableObject<>(leftInputVarRef));

        IFunctionInfo countFunc = context.getMetadataProvider().lookupFunction(BuiltinFunctions.COUNT);
        AggregateFunctionCallExpression countExpr = new AggregateFunctionCallExpression(countFunc, false, fields);
        countExpr.setSourceLocation(srcLoc);
        countExpr.setOpaqueParameters(new Object[] {});

        List<LogicalVariable> aggResultVar = new ArrayList<>(1);
        List<Mutable<ILogicalExpression>> aggFunc = new ArrayList<>(1);
        aggResultVar.add(context.newVar());
        aggFunc.add(new MutableObject<>(countExpr));

        AggregateOperator aggOp = EnforceStructuralPropertiesRule.createAggregate(aggResultVar, true, aggFunc, new MutableObject<>(replicateOperator.getOutputs().get(1).getValue()), context, srcLoc);
        MutableObject<ILogicalOperator> agg = new MutableObject<>(aggOp);

        // #3. Create the forward operator
        String aggKey = UUID.randomUUID().toString();
//        LogicalVariable aggVar = aggResultVar.get(0);
//        ForwardOperator forward = createForward(aggKey, null, exchToForwardRef, null, context, srcLoc);
//        ForwardOperator forward = createForward(aggKey, aggVar, exchToForwardRef, agg, context, srcLoc);
//        MutableObject<ILogicalOperator> forwardRef = new MutableObject<>(forward);
        ForwardOperator forwardOperator = new ForwardOperator(aggKey, new MutableObject<>(
            new ConstantExpression(new AsterixConstantValue(new AInt64(1)))));
        forwardOperator.setSourceLocation(srcLoc);
        forwardOperator.setPhysicalOperator(new SpatialForwardPOperator());
        forwardOperator.getInputs().add(new MutableObject<>(replicateOperator.getOutputs().get(0).getValue()));
        forwardOperator.getInputs().add(agg);
//        forwardOperator.getInputs().add(aggInput);
        OperatorManipulationUtil.setOperatorMode(forwardOperator);
        forwardOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(forwardOperator);

//        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator1);
//        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator2);*/


//        leftInputOp.setValue(forwardOperator);
//        context.computeAndSetTypeEnvironmentForOperator(replicateOperator);

/*
        // Add the dynamic MBR computation to the left branch
        SourceLocation srcLoc = op.getSourceLocation();
        // #1. Create the replicate operator and add it above the source op feeding parent operator
        ReplicateOperator replicateOp = EnforceStructuralPropertiesRule.createReplicateOperator(leftInputOp, 1, context, srcLoc);
        replicateOp.setSchema(leftInputOp.getValue().getSchema());
        context.computeAndSetTypeEnvironmentForOperator(replicateOp);

        // Add to exchange operator: one for the aggregate operator and one for the forward operator
        ExchangeOperator exchToForward = EnforceStructuralPropertiesRule.createOneToOneExchangeOp(new MutableObject<>(replicateOp), context);
        exchToForward.setSchema(replicateOp.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchToForward);
        MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchToForward);

//        ExchangeOperator exchToAgg = EnforceStructuralPropertiesRule.createOneToOneExchangeOp(new MutableObject<>(replicateOp), context);
//        MutableObject<ILogicalOperator> exchToAggRef = new MutableObject<>(exchToAgg);

        // Add the exchange-to-forward at output 0, the exchange-to-local-aggregate at output 1
        replicateOp.getOutputs().add(exchToForwardRef);
//        replicateOp.getOutputs().add(exchToAggRef);

        // Materialize the data to be able to re-read the data again after sampling is done
        replicateOp.getOutputMaterializationFlags()[0] = true;
//        leftInputOp.setValue(replicateOp);

//        op.recomputeSchema();
//        context.computeAndSetTypeEnvironmentForOperator(op);
/*
        // #2. Create the aggregate operator
        AbstractLogicalExpression leftInputVarRef = new VariableReferenceExpression(leftInputVar, srcLoc);
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
        fields.add(new MutableObject<>(leftInputVarRef));

        IFunctionInfo countFunc = context.getMetadataProvider().lookupFunction(BuiltinFunctions.COUNT);
        AggregateFunctionCallExpression countExpr = new AggregateFunctionCallExpression(countFunc, false, fields);
        countExpr.setSourceLocation(srcLoc);
        countExpr.setOpaqueParameters(new Object[] {});

        List<LogicalVariable> aggResultVar = new ArrayList<>(1);
        List<Mutable<ILogicalExpression>> aggFunc = new ArrayList<>(1);
        aggResultVar.add(context.newVar());
        aggFunc.add(new MutableObject<>(countExpr));

        AggregateOperator aggOp = EnforceStructuralPropertiesRule.createAggregate(aggResultVar, true, aggFunc, exchToAggRef, context, srcLoc);
        MutableObject<ILogicalOperator> agg = new MutableObject<>(aggOp);

        // #3. Create the forward operator
        String aggKey = UUID.randomUUID().toString();
        LogicalVariable aggVar = aggResultVar.get(0);
        ForwardOperator forward = createForward(aggKey, null, exchToForwardRef, null, context, srcLoc);
//        ForwardOperator forward = createForward(aggKey, aggVar, exchToForwardRef, agg, context, srcLoc);
        MutableObject<ILogicalOperator> forwardRef = new MutableObject<>(forward);

        // Replace the old input of parentOp requiring the range partitioning with the new forward op
        op.getInputs().set(LEFT, forwardRef);
        op.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(op);

        // Get left input again
        leftInputOp = op.getInputs().get(LEFT);*/

        // Compute reference tile ID
        ScalarFunctionCallExpression referenceTileId = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.REFERENCE_TILE),
                new MutableObject<>(new VariableReferenceExpression(leftInputVar)),
                new MutableObject<>(new VariableReferenceExpression(rightInputVar)),
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(
                        new ARectangle(new APoint(spatialJoinAnn.getMinX(), spatialJoinAnn.getMinY()),
                                new APoint(spatialJoinAnn.getMaxX(), spatialJoinAnn.getMaxY()))))),
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

        conditionExprs.add(new MutableObject<>(tileIdEquiJoinCondition));
        conditionExprs.add(new MutableObject<>(spatialJoinFuncExpr));
        conditionExprs.add(new MutableObject<>(referenceIdEquiJoinCondition));

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
        SpatialJoinUtils.setSpatialJoinOp(op, keysLeftBranch, keysRightBranch, context);
    }

    private static ForwardOperator createForward(String aggKey, LogicalVariable aggVariable,
                                                 MutableObject<ILogicalOperator> exchangeOpFromReplicate, MutableObject<ILogicalOperator> aggInput,
                                                 IOptimizationContext context, SourceLocation sourceLoc) throws AlgebricksException {
        AbstractLogicalExpression aggExpression = new VariableReferenceExpression(aggVariable, sourceLoc);
        ForwardOperator forwardOperator = new ForwardOperator(aggKey, new MutableObject<>(aggExpression));
//        ForwardOperator forwardOperator = new ForwardOperator(aggKey, new MutableObject<>(
//            new ConstantExpression(new AsterixConstantValue(new AInt64(1)))));
        forwardOperator.setSourceLocation(sourceLoc);
        forwardOperator.setPhysicalOperator(new SpatialForwardPOperator());
        forwardOperator.getInputs().add(exchangeOpFromReplicate);
        forwardOperator.getInputs().add(aggInput);
        OperatorManipulationUtil.setOperatorMode(forwardOperator);
        forwardOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(forwardOperator);
        return forwardOperator;
    }
}
