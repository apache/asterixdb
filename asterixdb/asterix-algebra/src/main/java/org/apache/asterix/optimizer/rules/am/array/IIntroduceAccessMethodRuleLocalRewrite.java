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

package org.apache.asterix.optimizer.rules.am.array;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;

public interface IIntroduceAccessMethodRuleLocalRewrite<T> {
    /**
     * @param originalOperator Original operator before rewrite. Should be returned by {@code restoreBeforeRewrite}.
     * @param context Optimization context.
     * @return Null if no rewrite has occurred. Otherwise, a new plan from the given operator.
     * @throws AlgebricksException
     */
    T createOperator(T originalOperator, IOptimizationContext context) throws AlgebricksException;

    /**
     * @param afterOperatorRefs Operators after the original operator that should be restored after the rewrite.
     * @param context Optimization context.
     * @return The original operator given at {@code createOperator} time.
     * @throws AlgebricksException
     */
    T restoreBeforeRewrite(List<Mutable<ILogicalOperator>> afterOperatorRefs, IOptimizationContext context)
            throws AlgebricksException;
}
