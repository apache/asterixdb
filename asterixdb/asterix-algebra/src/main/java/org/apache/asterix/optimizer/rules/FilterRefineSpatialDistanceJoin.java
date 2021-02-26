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

import java.util.List;

import org.apache.asterix.common.annotations.SpatialJoinAnnotation;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * If the join condition is st_distance() and required annotation is provided,
 * this rule applies the spatial join  into the query by adding the spatial-intersect function and
 * sends the extended mbr of the geometries to it.
 *
 * For example:<br/>
 *
 * SELECT COUNT(*) FROM ParkSet AS ps, LakeSet AS ls
 * WHERE /*+ spatial-partitioning -180.0 -83.0 180.0 90.0 10 10 &#42;/ st_distance(ps.geom,ls.geom) < 100.5;
 *
 * Becomes,
 *
 * SELECT COUNT(*) FROM ParkSet AS ps, LakeSet AS ls
 * WHERE /*+ spatial-partitioning -180.0 -83.0 180.0 90.0 10 10 &#42;/
 * spatial_intersect(st_mbr_offset(ps.geom, 100.5),st_mbr(ls.geom)) and st_distance(ps.geom,ls.geom) < 100.5;
 *
 * Note: st_mbr() computes the mbr of a Geometry, and
 * st_mbr_offset() computes the mbr of a Geometry and extending by the second parameter.
 */
public class FilterRefineSpatialDistanceJoin implements IAlgebraicRewriteRule {

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

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }

        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        Mutable<ILogicalExpression> joinConditionRef = joinOp.getCondition();
        ILogicalExpression joinCondition = joinConditionRef.getValue();

        if (joinCondition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) joinCondition;
        if (!(funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.LT)
                || funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.LE)
                || funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.EQ))) {
            return false;
        }

        List<Mutable<ILogicalExpression>> inputExprs = funcExpr.getArguments();

        ILogicalExpression leftOperatingExpr = inputExprs.get(LEFT).getValue();
        ILogicalExpression rightOperatingExpr = inputExprs.get(RIGHT).getValue();

        if (leftOperatingExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL
                || rightOperatingExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }

        AbstractFunctionCallExpression distanceFuncCallExpr =
                (AbstractFunctionCallExpression) inputExprs.get(LEFT).getValue();
        ConstantExpression distanceValExpr = (ConstantExpression) inputExprs.get(RIGHT).getValue();

        if (!distanceFuncCallExpr.getFunctionIdentifier().equals(BuiltinFunctions.ST_DISTANCE)
                || distanceFuncCallExpr.getAnnotation(SpatialJoinAnnotation.class) == null) {
            return false;
        }

        // Left and right arguments of the st_distance function should be either variable or function call.
        List<Mutable<ILogicalExpression>> distanceFuncCallArgs = distanceFuncCallExpr.getArguments();
        Mutable<ILogicalExpression> distanceFuncCallLeftArg = distanceFuncCallArgs.get(LEFT);
        Mutable<ILogicalExpression> distanceFuncCallRightArg = distanceFuncCallArgs.get(RIGHT);
        if (distanceFuncCallLeftArg.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT
                || distanceFuncCallRightArg.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return false;
        }

        // Enlarge the MBR of the left argument of the refine function (st_distance)
        IAlgebricksConstantValue distanceVar = distanceValExpr.getValue();
        ScalarFunctionCallExpression enlargedLeft = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.ST_MBR_OFFSET), distanceFuncCallLeftArg,
                new MutableObject<>(new ConstantExpression(distanceVar)));
        // Compute the MBR of the right argument of the refine function (st_distance)
        ScalarFunctionCallExpression rightMBR = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.ST_MBR), distanceFuncCallRightArg);

        // Create filter function (spatial_intersect)
        ScalarFunctionCallExpression spatialIntersect = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_INTERSECT),
                new MutableObject<>(enlargedLeft), new MutableObject<>(rightMBR));
        // Attach the annotation to the spatial_intersect function
        spatialIntersect.putAnnotation(distanceFuncCallExpr.getAnnotation(SpatialJoinAnnotation.class));

        // Update join condition with filter and refine function
        ScalarFunctionCallExpression updatedJoinCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND),
                        new MutableObject<>(spatialIntersect), new MutableObject<>(funcExpr));

        joinConditionRef.setValue(updatedJoinCondition);

        return true;
    }

}
