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

package org.apache.hyracks.algebricks.rewriter.rules;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * The rule switches the left branch of a join and the right branch if
 * we know the left branch has cardinality one and the right branch does
 * not have cardinality one. Therefore, the build (right) branch can
 * potentially be smaller.
 */
public class SwitchInnerJoinBranchRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }
        InnerJoinOperator joinOperator = (InnerJoinOperator) op;
        Mutable<ILogicalOperator> leftRef = joinOperator.getInputs().get(0);
        Mutable<ILogicalOperator> rightRef = joinOperator.getInputs().get(1);
        ILogicalOperator left = leftRef.getValue();
        ILogicalOperator right = rightRef.getValue();
        boolean leftCardinalityOne = OperatorPropertiesUtil.isCardinalityZeroOrOne(left);
        boolean rightCardinalityOne = OperatorPropertiesUtil.isCardinalityZeroOrOne(right);
        if (!leftCardinalityOne || rightCardinalityOne) {
            return false;
        }
        // The cardinality of the left branch is one and the cardinality of the right branch is not one.
        leftRef.setValue(right);
        rightRef.setValue(left);
        return true;
    }
}
