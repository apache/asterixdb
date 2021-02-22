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

import org.apache.asterix.common.annotations.RangeAnnotation;
import org.apache.asterix.common.annotations.SpatialJoinAnnotation;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortForwardPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SpatialForwardPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.rewriter.rules.EnforceStructuralPropertiesRule;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class AsterixJoinUtils {

    private static final int LEFT = 0;
    private static final int RIGHT = 1;

    private AsterixJoinUtils() {
    }

    public static void setJoinAlgorithmAndExchangeAlgo(AbstractBinaryJoinOperator op, Boolean topLevelOp,
            IOptimizationContext context) throws AlgebricksException {
        if (!topLevelOp) {
            return;
        }
        ILogicalExpression conditionLE = op.getCondition().getValue();
        if (conditionLE.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }

        List<LogicalVariable> sideLeft = new ArrayList<>(1);
        List<LogicalVariable> sideRight = new ArrayList<>(1);
        List<LogicalVariable> varsLeft = op.getInputs().get(LEFT).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(RIGHT).getValue().getSchema();

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) conditionLE;
        FunctionIdentifier fi =
            IntervalJoinUtils.isIntervalJoinCondition(funcExpr, varsLeft, varsRight, sideLeft, sideRight, LEFT, RIGHT);
        if (fi != null) {
            // Existing workflow for interval merge join
            RangeAnnotation rangeAnnotation = IntervalJoinUtils.findRangeAnnotation(funcExpr);
            if (rangeAnnotation == null) {
                return;
            }
            //Check RangeMap type
            RangeMap rangeMap = rangeAnnotation.getRangeMap();
            if (rangeMap.getTag(0, 0) != ATypeTag.DATETIME.serialize() && rangeMap.getTag(0, 0) != ATypeTag.DATE.serialize()
                && rangeMap.getTag(0, 0) != ATypeTag.TIME.serialize()) {
                IWarningCollector warningCollector = context.getWarningCollector();
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(Warning.forHyracks(op.getSourceLocation(), ErrorCode.INAPPLICABLE_HINT,
                        "Date, DateTime, and Time are only range hints types supported for interval joins"));
                }
                return;
            }
            IntervalPartitions intervalPartitions =
                IntervalJoinUtils.createIntervalPartitions(op, fi, sideLeft, sideRight, rangeMap, context, LEFT, RIGHT);
            IntervalJoinUtils.setSortMergeIntervalJoinOp(op, fi, sideLeft, sideRight, context, intervalPartitions);
        } else {
            // Check if the join condition contains spatial join
            Mutable<ILogicalExpression> joinConditionRef = op.getCondition();
            SpatialJoinAnnotation spatialJoinAnn = null;
            AbstractFunctionCallExpression spatialJoinFuncExpr = null;
            List<Mutable<ILogicalExpression>> conditionExprs = new ArrayList<>();

            if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.AND)) {
                // Join condition contains multiple conditions along with spatial_intersect
                List<Mutable<ILogicalExpression>> inputExprs = funcExpr.getArguments();
                if (inputExprs.size() == 0) {
                    return;
                }

                boolean spatialFunctionCallExists = false;
                for (Mutable<ILogicalExpression> exp : inputExprs) {
                    AbstractFunctionCallExpression funcCallExp = (AbstractFunctionCallExpression) exp.getValue();
                    if (funcCallExp.getFunctionIdentifier().equals(BuiltinFunctions.SPATIAL_INTERSECT)) {
                        spatialJoinFuncExpr = funcCallExp;
                        spatialFunctionCallExists = true;
                    } else {
                        conditionExprs.add(exp);
                        if (BuiltinFunctions.isSTFilterRefineFunction(funcCallExp.getFunctionIdentifier())) {
                            spatialJoinAnn = funcCallExp.getAnnotation(SpatialJoinAnnotation.class);
                        }
                    }
                }

                if (!spatialFunctionCallExists) {
                    return;
                }
            } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.SPATIAL_INTERSECT)) {
                // Join condition is spatial_intersect only
                spatialJoinFuncExpr = funcExpr;
                spatialJoinAnn = spatialJoinFuncExpr.getAnnotation(SpatialJoinAnnotation.class);
            } else {
                return;
            }

            if (spatialJoinAnn == null) {
//                spatialJoinAnn = new SpatialJoinAnnotation(-180.0, -83.0, 180, 90.0, 10, 10);
                // Spatial annotation is mandatory to apply spatial join optimal rules
                return;
            }

            // Extracts spatial intersect function's arguments
            List<Mutable<ILogicalExpression>> spatialJoinInputExprs = spatialJoinFuncExpr.getArguments();
            if (spatialJoinInputExprs.size() != 2) {
                return;
            }

            ILogicalExpression leftOperatingExpr = spatialJoinInputExprs.get(LEFT).getValue();
            ILogicalExpression rightOperatingExpr = spatialJoinInputExprs.get(RIGHT).getValue();

            // left and right expressions should be variables.
            if (leftOperatingExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT
                || rightOperatingExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                return;
            }

            // Gets both input branches of the spatial join.
            Mutable<ILogicalOperator> leftOp = op.getInputs().get(LEFT);
            Mutable<ILogicalOperator> rightOp = op.getInputs().get(RIGHT);

            // Add dynamic spatial partitioning workflow
            SourceLocation srcLoc = op.getSourceLocation();

            // #1. create the replicate operator and add it above the source op feeding parent operator
            // Left
            ReplicateOperator leftReplicateOp = EnforceStructuralPropertiesRule.createReplicateOperator(leftOp, context, srcLoc);

            // these two exchange ops are needed so that the parents of replicate stay the same during later optimizations.
            // This is because replicate operator has references to its parents. If any later optimizations add new parents,
            // then replicate would still point to the old ones.
            ExchangeOperator exchToLocalAgg = EnforceStructuralPropertiesRule.createOneToOneExchangeOp(new MutableObject<>(leftReplicateOp), context);
            ExchangeOperator exchToForward = EnforceStructuralPropertiesRule.createOneToOneExchangeOp(new MutableObject<>(leftReplicateOp), context);
            MutableObject<ILogicalOperator> exchToAggRef = new MutableObject<>(exchToLocalAgg);
            MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchToForward);

            // add the exchange-to-forward at output 0, the exchange-to-local-aggregate at output 1
            leftReplicateOp.getOutputs().add(exchToForwardRef);
            leftReplicateOp.getOutputs().add(exchToAggRef);
            // materialize the data to be able to re-read the data again after sampling is done
            leftReplicateOp.getOutputMaterializationFlags()[0] = true;

            // #2. create the aggregate operators and their sampling functions
            List<LogicalVariable> aggResultVar = new ArrayList<>(1);
            List<Mutable<ILogicalExpression>> aggFunc = new ArrayList<>(1);
//            createAggregateFunction(context, aggResultVar, aggFunc, srcLoc);
            IFunctionInfo countFunc = context.getMetadataProvider().lookupFunction(BuiltinFunctions.COUNT);
            List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
            AggregateFunctionCallExpression countExpr = new AggregateFunctionCallExpression(countFunc, false, fields);
            countExpr.setSourceLocation(srcLoc);
            aggResultVar.add(context.newVar());
            aggFunc.add(new MutableObject<>(countExpr));
            AggregateOperator aggOp = EnforceStructuralPropertiesRule.createAggregate(aggResultVar, true, aggFunc, exchToAggRef, context, srcLoc);
            MutableObject<ILogicalOperator> agg = new MutableObject<>(aggOp);

            // #3. create the forward operator
            String aggKey = UUID.randomUUID().toString();
            LogicalVariable aggVar = aggResultVar.get(0);
            ForwardOperator forward = createForward(aggKey, aggVar, exchToForwardRef, agg, context, srcLoc);
            MutableObject<ILogicalOperator> forwardRef = new MutableObject<>(forward);

            // replace the old input of parentOp requiring the range partitioning with the new forward op
            op.getInputs().set(LEFT, forwardRef);
            op.recomputeSchema();
            context.computeAndSetTypeEnvironmentForOperator(op);

            // Right
//            ReplicateOperator rightReplicateOp = EnforceStructuralPropertiesRule.createReplicateOperator(rightOp, context, srcLoc);

            // Extract left and right variable of the predicate
            LogicalVariable leftInputVar = ((VariableReferenceExpression) leftOperatingExpr).getVariableReference();
            LogicalVariable rightInputVar = ((VariableReferenceExpression) rightOperatingExpr).getVariableReference();

            // Inject unnest operator to the left and right branch of the join operator
            leftOp = op.getInputs().get(LEFT);
            LogicalVariable leftTileIdVar = SpatialJoinUtils.injectUnnestOperator(context, leftOp, leftInputVar, spatialJoinAnn);
            LogicalVariable rightTileIdVar = SpatialJoinUtils.injectUnnestOperator(context, rightOp, rightInputVar, spatialJoinAnn);

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
            joinConditionRef.setValue(updatedJoinCondition);

            List<LogicalVariable> keysLeftBranch = new ArrayList<>();
            keysLeftBranch.add(leftTileIdVar);
            keysLeftBranch.add(leftInputVar);
            List<LogicalVariable> keysRightBranch = new ArrayList<>();
            keysRightBranch.add(rightTileIdVar);
            keysRightBranch.add(rightInputVar);
            SpatialJoinUtils.setSpatialJoinOp(op, keysLeftBranch, keysRightBranch, context);
        }
    }

    private static void createAggregateFunction(IOptimizationContext context, List<LogicalVariable> globalResultVariable,
                                                List<Mutable<ILogicalExpression>> globalAggFunction, SourceLocation sourceLocation) {
//        IFunctionInfo countFunc = context.getMetadataProvider().lookupFunction(BuiltinFunctions.COUNT);
//        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
//        AggregateFunctionCallExpression countExpr = new AggregateFunctionCallExpression(countFunc, false, fields);
//        countExpr.setSourceLocation(sourceLocation);
//        globalResultVariable.add(context.newVar());
//        globalAggFunction.add(new MutableObject<>(countExpr));
    }

    private static ForwardOperator createForward(String aggKey, LogicalVariable aggVariable,
                                                 MutableObject<ILogicalOperator> exchangeOpFromReplicate, MutableObject<ILogicalOperator> aggInput,
                                                 IOptimizationContext context, SourceLocation sourceLoc) throws AlgebricksException {
        AbstractLogicalExpression aggExpression = new VariableReferenceExpression(aggVariable, sourceLoc);
        ForwardOperator forwardOperator = new ForwardOperator(aggKey, new MutableObject<>(aggExpression));
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
