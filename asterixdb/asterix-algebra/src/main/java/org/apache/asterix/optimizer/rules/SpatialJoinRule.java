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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

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
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SpatialJoinRule implements IAlgebraicRewriteRule {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int LEFT = 0;
    private static final int RIGHT = 1;

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

        SpatialJoinAnnotation spatialJoinAnn = null;
        List<Mutable<ILogicalExpression>> conditionExprs = new ArrayList<>();
        AbstractFunctionCallExpression spatialJoinFuncExpr = null;
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) joinCondition;
        if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.AND)) {
            List<Mutable<ILogicalExpression>> inputExprs = funcExpr.getArguments();
            if (inputExprs.size() == 0) {
                return false;
            }

            boolean spatialFunctionCallExists = false;
            for (Mutable<ILogicalExpression> exp : inputExprs) {
                AbstractFunctionCallExpression funcCallExp = (AbstractFunctionCallExpression) exp.getValue();
                if (funcCallExp.getFunctionIdentifier().equals(BuiltinFunctions.SPATIAL_INTERSECT)) {
                    spatialJoinFuncExpr = funcCallExp;
                    spatialFunctionCallExists = true;
                } else {
                    conditionExprs.add(exp);
                    if (funcCallExp.getFunctionIdentifier().equals(BuiltinFunctions.ST_INTERSECTS)) {
                        spatialJoinAnn = funcCallExp.getAnnotation(SpatialJoinAnnotation.class);
                    }
                }
            }

            if (!spatialFunctionCallExists) {
                return false;
            }
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.SPATIAL_INTERSECT)) {
            spatialJoinFuncExpr = funcExpr;
            spatialJoinAnn = spatialJoinFuncExpr.getAnnotation(SpatialJoinAnnotation.class);
        } else {
            return false;
        }

        if (spatialJoinAnn == null) {
            spatialJoinAnn = new SpatialJoinAnnotation(-180.0, -83.0, 180, 90.0, 10, 10);
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
        LogicalVariable leftTileIdVar = injectUnnestOperator(context, leftOp, leftInputVar, spatialJoinAnn);
        LogicalVariable rightTileIdVar = injectUnnestOperator(context, rightOp, rightInputVar, spatialJoinAnn);

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
        //        ScalarFunctionCallExpression spatialJoinCondition = new ScalarFunctionCallExpression(
        //                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_INTERSECT),
        //                new MutableObject<>(new VariableReferenceExpression(leftInputVar)),
        //                new MutableObject<>(new VariableReferenceExpression(rightInputVar)));

        conditionExprs.add(new MutableObject<>(spatialJoinFuncExpr));
        conditionExprs.add(new MutableObject<>(tileIdEquiJoinCondition));
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
        ISpatialJoinUtilFactory mjcf = new IntersectSpatialJoinUtilFactory();
        joinOp.setPhysicalOperator(new SpatialJoinPOperator(joinOp.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.PAIRWISE, keysLeftBranch, keysRightBranch,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), mjcf));

        context.addToDontApplySet(this, op);
        return true;
    }

    private LogicalVariable injectUnnestOperator(IOptimizationContext context, Mutable<ILogicalOperator> sideOp,
            LogicalVariable inputVar, SpatialJoinAnnotation spatialJoinAnn) {
        LogicalVariable sideVar = context.newVar();
        VariableReferenceExpression sideInputVar = new VariableReferenceExpression(inputVar);
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
}
