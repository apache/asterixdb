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
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * If the join condition is one of the below Geometry functions and the required annotation is provided,
 * this rule applies the spatial join by adding the spatial-intersect function into the query.
 *
 * <ul>
 *     <li>st_intersects</li>
 *     <li>st_overlaps</li>
 *     <li>st_touches</li>
 *     <li>st_contains</li>
 *     <li>st_crosses</li>
 *     <li>st_within</li>
 * </ul>
 *
 * For example:<br/>
 *
 * SELECT COUNT(*) FROM ParkSet AS ps, LakeSet AS ls
 * WHERE /*+ spatial-partitioning -180.0 -83.0 180.0 90.0 10 10 &#42;/ st_intersects(ps.geom,ls.geom);
 *
 * Becomes,
 *
 * SELECT COUNT(*) FROM ParkSet AS ps, LakeSet AS ls
 * WHERE /*+ spatial-partitioning -180.0 -83.0 180.0 90.0 10 10 &#42;/
 * spatial_intersect(st_mbr(ps.geom),st_mbr(ls.geom)) and st_intersects(ps.geom,ls.geom);
 *
 * Note: st_mbr($x, $y) computes the mbr of the geometry and returns rectangles to pass it spatial_intersect($x, $y)
 *
 */
public class FilterRefineSpatialJoin implements IAlgebraicRewriteRule {

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

        AbstractFunctionCallExpression stFuncExpr = (AbstractFunctionCallExpression) joinCondition;
        if ((stFuncExpr.getAnnotation(SpatialJoinAnnotation.class) == null)
                || !BuiltinFunctions.isSTFilterRefineFunction(stFuncExpr.getFunctionIdentifier())) {
            return false;
        }

        // Left and right arguments of the refine function should be either variable or function call.
        List<Mutable<ILogicalExpression>> stFuncArgs = stFuncExpr.getArguments();
        Mutable<ILogicalExpression> stFuncLeftArg = stFuncArgs.get(LEFT);
        Mutable<ILogicalExpression> stFuncRightArg = stFuncArgs.get(RIGHT);
        if (stFuncLeftArg.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT
                || stFuncRightArg.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return false;
        }

        // Compute MBRs of the left and right arguments of the refine function
        ScalarFunctionCallExpression left = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.ST_MBR), stFuncLeftArg);
        ScalarFunctionCallExpression right = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.ST_MBR), stFuncRightArg);

        // Create filter function (spatial_intersect)
        ScalarFunctionCallExpression spatialIntersect = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_INTERSECT), new MutableObject<>(left),
                new MutableObject<>(right));
        // Attach the annotation to the spatial_intersect function
        spatialIntersect.putAnnotation(stFuncExpr.getAnnotation(SpatialJoinAnnotation.class));

        // Update join condition with filter and refine function
        ScalarFunctionCallExpression updatedJoinCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND),
                        new MutableObject<>(spatialIntersect), new MutableObject<>(stFuncExpr));
        joinConditionRef.setValue(updatedJoinCondition);

        return true;
    }

}
