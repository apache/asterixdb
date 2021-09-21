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
 * If the join condition is one of the below Geometry functions,
 * this rule applies the spatial join by adding the spatial-intersect condition into the join predicate.
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
 *
 * For example:<br/>
 *
 * join (st-intersects($$50, $$51)) -- |UNPARTITIONED|
 *   assign [$$50] <- [$$ps.getField(1)] -- |UNPARTITIONED|
 *     data-scan []<-[$$47, $$ps] <- test.ParkSetG -- |UNPARTITIONED|
 *       empty-tuple-source -- |UNPARTITIONED|
 *   assign [$$51] <- [$$ls.getField(1)] -- |UNPARTITIONED|
 *     data-scan []<-[$$48, $$ls] <- test.LakeSetG -- |UNPARTITIONED|
 *       empty-tuple-source -- |UNPARTITIONED|
 *
 * Becomes,
 *
 * join (and(spatial-intersect(st-mbr($$50), st-mbr($$51)), st-intersects($$50, $$51))) -- |UNPARTITIONED|
 *   assign [$$50] <- [$$ps.getField(1)] -- |UNPARTITIONED|
 *     data-scan []<-[$$47, $$ps] <- test.ParkSetG -- |UNPARTITIONED|
 *       empty-tuple-source -- |UNPARTITIONED|
 *   assign [$$51] <- [$$ls.getField(1)] -- |UNPARTITIONED|
 *     data-scan []<-[$$48, $$ls] <- test.LakeSetG -- |UNPARTITIONED|
 *       empty-tuple-source -- |UNPARTITIONED|
 *
 *  st-mbr($x, $y) computes the mbr of the geometry and returns rectangles to pass it spatial_intersect($x, $y)
 *
 *  The /*+ spatial-partitioning(x1, y1, x2, y2, row, col) &#42;/ annotation allows users to define the MBR and
 *  grid size (row,col) which are used for partitioning. If the query does not have an annotation, the MBR is computed
 *  dynamically and grid size set to 100 100.
 */
public class FilterRefineSpatialJoinRuleForSTFunctions implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }

        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        Mutable<ILogicalExpression> joinConditionRef = joinOp.getCondition();
        ILogicalExpression joinCondition = joinConditionRef.getValue();

        if (joinCondition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression stFuncExpr = (AbstractFunctionCallExpression) joinCondition;
        if (!BuiltinFunctions.isSTFilterRefineFunction(stFuncExpr.getFunctionIdentifier())) {
            return false;
        }

        // Left and right arguments of the refine function should be either variable or function call.
        List<Mutable<ILogicalExpression>> stFuncArgs = stFuncExpr.getArguments();
        Mutable<ILogicalExpression> stFuncLeftArg = stFuncArgs.get(0);
        Mutable<ILogicalExpression> stFuncRightArg = stFuncArgs.get(1);
        if (stFuncLeftArg.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT
                || stFuncRightArg.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return false;
        }

        // Compute MBRs of the left and right arguments of the refine function
        ScalarFunctionCallExpression left = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.ST_MBR), stFuncLeftArg);
        left.setSourceLocation(stFuncLeftArg.getValue().getSourceLocation());
        ScalarFunctionCallExpression right = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.ST_MBR), stFuncRightArg);
        right.setSourceLocation(stFuncRightArg.getValue().getSourceLocation());

        // Create filter function (spatial_intersect)
        ScalarFunctionCallExpression spatialIntersect = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_INTERSECT),
                new MutableObject<>(left.cloneExpression()), new MutableObject<>(right.cloneExpression()));
        spatialIntersect.setSourceLocation(op.getSourceLocation());

        // Attach the annotation to the spatial_intersect function if it is available
        if (stFuncExpr.getAnnotation(SpatialJoinAnnotation.class) != null) {
            spatialIntersect.putAnnotation(stFuncExpr.getAnnotation(SpatialJoinAnnotation.class));
        }

        // Update join condition with filter and refine function
        ScalarFunctionCallExpression updatedJoinCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND),
                        new MutableObject<>(spatialIntersect), new MutableObject<>(stFuncExpr));
        updatedJoinCondition.setSourceLocation(op.getSourceLocation());
        joinConditionRef.setValue(updatedJoinCondition);

        return true;
    }

}
