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
package org.apache.asterix.optimizer.rules.am;

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

/**
 * A disjunction of equalities between one variable and constants of one type, e.g. an IN list, as a single
 * optimizable expression rather than one per disjunct.
 * <p>
 * {@link #getFuncExpr()} is the first disjunct, so that callers which inspect the comparison or its annotations see
 * an equality; {@link #getDisjunction()} is the whole disjunction. The constants are those of the disjuncts with
 * duplicates removed, the first disjunct's constant at index 0.
 */
public class DisjunctiveEqualityFuncExpr extends OptimizableFuncExpr {

    private final AbstractFunctionCallExpression disjunction;

    public DisjunctiveEqualityFuncExpr(AbstractFunctionCallExpression disjunction,
            AbstractFunctionCallExpression firstDisjunct, LogicalVariable logicalVar, int varIndexInFirstDisjunct,
            ILogicalExpression[] constantExpressions, IAType[] constantExpressionTypes) {
        super(firstDisjunct, new LogicalVariable[] { logicalVar }, new int[] { varIndexInFirstDisjunct },
                constantExpressions, constantExpressionTypes);
        this.disjunction = disjunction;
    }

    public AbstractFunctionCallExpression getDisjunction() {
        return disjunction;
    }
}
