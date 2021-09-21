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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;

public class AsterixJoinUtils {

    private AsterixJoinUtils() {
    }

    public static void setJoinAlgorithmAndExchangeAlgo(AbstractBinaryJoinOperator op, Boolean topLevelOp,
            IOptimizationContext context) throws AlgebricksException {
        if (!topLevelOp) {
            return;
        }
        ILogicalExpression joinCondition = op.getCondition().getValue();
        if (joinCondition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }

        boolean intervalJoinChosen = IntervalJoinUtils.tryIntervalJoinAssignment(op, context, joinCondition, 0, 1);
        if (!intervalJoinChosen) {
            SpatialJoinUtils.trySpatialJoinAssignment(op, context, joinCondition, 0, 1);
        }
    }
}
