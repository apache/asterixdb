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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SpatialJoinPOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SpatialJoinRule implements IAlgebraicRewriteRule {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int LEFT = 0;
    private static final int RIGHT = 1;
    private static final double MIN_X = -180.0;
    private static final double MIN_Y = -83.0;
    private static final double MAX_X = 180.0;
    private static final double MAX_Y = 90.0;
    private static final int NUM_ROWS = 50;
    private static final int NUM_COLUMNS = 50;

    private final Integer64SerializerDeserializer integerSerde = Integer64SerializerDeserializer.INSTANCE;
    private static final int INTEGER_LENGTH = Long.BYTES;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // Current operator should be a join.
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }

        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        // Finds SPATIAL_INTERSECT function in the join condition.
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        Mutable<ILogicalExpression> joinConditionRef = joinOp.getCondition();
        ILogicalExpression joinCondition = joinConditionRef.getValue();

        if (joinCondition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression spatialJoinFuncExpr;
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) joinCondition;
        if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.AND)) {
            List<Mutable<ILogicalExpression>> inputExprs = funcExpr.getArguments();
            if (inputExprs.size() != 2) {
                return false;
            }
            spatialJoinFuncExpr = (AbstractFunctionCallExpression) inputExprs.get(0).getValue();
            if (!spatialJoinFuncExpr.getFunctionIdentifier().equals(BuiltinFunctions.SPATIAL_INTERSECT)) {
                return false;
            }
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.SPATIAL_INTERSECT)) {
            spatialJoinFuncExpr = funcExpr;
        } else {
            return false;
        }

        // Extracts spatial intersect function's arguments
        List<Mutable<ILogicalExpression>> spatialJoinInputExprs = spatialJoinFuncExpr.getArguments();
        if (spatialJoinInputExprs.size() != 2) {
            return false;
        }

        ILogicalExpression leftOperatingExpr = spatialJoinInputExprs.get(LEFT).getValue();
        ILogicalExpression rightOperatingExpr = spatialJoinInputExprs.get(RIGHT).getValue();

        // left and right expressions should be variables.
        if (leftOperatingExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT
                || rightOperatingExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return false;
        }

        LOGGER.info("spatial-intersect is called");
        //        return false;

        // Gets both input branches of the spatial join.
        Mutable<ILogicalOperator> leftOp = joinOp.getInputs().get(LEFT);
        Mutable<ILogicalOperator> rightOp = joinOp.getInputs().get(RIGHT);

        // Extract left and right variable of the predicate
        LogicalVariable inputVar0 = ((VariableReferenceExpression) leftOperatingExpr).getVariableReference();
        LogicalVariable inputVar1 = ((VariableReferenceExpression) rightOperatingExpr).getVariableReference();

        LogicalVariable leftInputVar;
        LogicalVariable rightInputVar;
        Collection<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(leftOp.getValue(), liveVars);
        if (liveVars.contains(inputVar0)) {
            leftInputVar = inputVar0;
            rightInputVar = inputVar1;
        } else {
            leftInputVar = inputVar1;
            rightInputVar = inputVar0;
        }

        // Inject unnest operator to the left and right branch of the join operator
        LogicalVariable leftTileIdVar = injectUnnestOperator(context, leftOp, leftInputVar);
        LogicalVariable rightTileIdVar = injectUnnestOperator(context, rightOp, rightInputVar);

        // Compute reference tile ID
        ScalarFunctionCallExpression referenceTileId =
                new ScalarFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.REFERENCE_TILE),
                        new MutableObject<>(new VariableReferenceExpression(leftInputVar)),
                        new MutableObject<>(new VariableReferenceExpression(rightInputVar)),
                        new MutableObject<>(new ConstantExpression(new AsterixConstantValue(
                                new ARectangle(new APoint(MIN_X, MIN_Y), new APoint(MAX_X, MAX_Y))))),
                        new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(NUM_ROWS)))),
                        new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(NUM_COLUMNS)))));

        // Update the join conditions with the tile Id equality condition
        ScalarFunctionCallExpression tileIdEquiJoinCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.EQ),
                        new MutableObject<>(new VariableReferenceExpression(leftTileIdVar)),
                        new MutableObject<>(new VariableReferenceExpression(rightTileIdVar)));
        ScalarFunctionCallExpression referenceIdEquiJoinCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.EQ),
                        new MutableObject<>(new VariableReferenceExpression(leftTileIdVar)),
                        new MutableObject<>(referenceTileId));
        ScalarFunctionCallExpression spatialJoinCondition = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_INTERSECT),
                new MutableObject<>(new VariableReferenceExpression(leftInputVar)),
                new MutableObject<>(new VariableReferenceExpression(rightInputVar)));
        ScalarFunctionCallExpression updatedJoinCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND),
                        new MutableObject<>(spatialJoinCondition), new MutableObject<>(tileIdEquiJoinCondition),
                        new MutableObject<>(referenceIdEquiJoinCondition));
        joinConditionRef.setValue(updatedJoinCondition);

        joinOp.setPhysicalOperator(new SpatialJoinPOperator(joinOp.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));

        // This one uses HYBRID_HASH_JOIN: tile1 == tile2
        // Force the planner to use BNLJ:

        //        ScalarFunctionCallExpression spatialJoinCondition = new ScalarFunctionCallExpression(
        //                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.INTERVAL_OVERLAPS), // TODO: change to SPATIAL_INTERSECTS
        //                new MutableObject<>(new VariableReferenceExpression(leftInputVar)),
        //                new MutableObject<>(new VariableReferenceExpression(rightInputVar)));
        //        joinConditionRef.setValue(spatialJoinCondition);

        // Force the query plan to use Interval merge join
        //        List<LogicalVariable> sideLeft = new ArrayList<>(1);
        //        sideLeft.add(leftInputVar);
        //        List<LogicalVariable> sideRight = new ArrayList<>(1);
        //        sideRight.add(rightInputVar);
        //        Long[] integers = new Long[1];
        //        integers[0] = Long.valueOf(-100);
        //        RangeMap rangeMap = null;
        //        try {
        //            rangeMap = getIntegerRangeMap(integers);
        //        } catch (HyracksDataException e) {
        //            e.printStackTrace();
        //        }
        //        IntervalPartitions intervalPartitions = IntervalJoinUtils.createIntervalPartitions(joinOp,
        //                funcExpr.getFunctionIdentifier(), sideLeft, sideRight, rangeMap, context, LEFT, RIGHT);
        //        IntervalJoinUtils.setSortMergeIntervalJoinOp(joinOp, funcExpr.getFunctionIdentifier(), sideLeft, sideRight,
        //                context, intervalPartitions);

        context.addToDontApplySet(this, op);
        return true;
    }

    private LogicalVariable injectUnnestOperator(IOptimizationContext context, Mutable<ILogicalOperator> sideOp,
            LogicalVariable inputVar) {
        LogicalVariable sideVar = context.newVar();
        VariableReferenceExpression sideInputVar = new VariableReferenceExpression(inputVar);
        UnnestingFunctionCallExpression funcExpr =
                new UnnestingFunctionCallExpression(
                        BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_TILE),
                        new MutableObject<>(sideInputVar),
                        new MutableObject<>(new ConstantExpression(new AsterixConstantValue(
                                new ARectangle(new APoint(MIN_X, MIN_Y), new APoint(MAX_X, MAX_Y))))),
                        new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(NUM_ROWS)))),
                        new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(NUM_COLUMNS)))));
        funcExpr.setSourceLocation(sideOp.getValue().getSourceLocation());
        UnnestOperator sideUnnestOp = new UnnestOperator(sideVar, new MutableObject<>(funcExpr));
        sideUnnestOp.getInputs().add(new MutableObject<>(sideOp.getValue()));
        sideOp.setValue(sideUnnestOp);
        try {
            context.computeAndSetTypeEnvironmentForOperator(sideUnnestOp);
        } catch (AlgebricksException e) {
            e.printStackTrace();
        }
        return sideVar;
    }

    private RangeMap getIntegerRangeMap(Long[] integers) throws HyracksDataException {
        int[] offsets = new int[integers.length];
        for (int i = 0; i < integers.length; ++i) {
            offsets[i] = (i + 1) * INTEGER_LENGTH;
        }
        return new RangeMap(1, getIntegerBytes(integers), offsets, null);
    }

    private byte[] getIntegerBytes(Long[] integers) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < integers.length; ++i) {
                integerSerde.serialize(integers[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
