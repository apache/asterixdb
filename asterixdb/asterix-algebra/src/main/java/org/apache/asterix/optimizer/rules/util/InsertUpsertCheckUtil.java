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

import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

public class InsertUpsertCheckUtil {

    private InsertUpsertCheckUtil() {
    }

    /**
     *
     * Checks the query plan rooted at <code>op</code> to see whether there is an invalid returning expression
     * for insert/upsert, i.e., an returning expression that contains dataset accesses.
     *
     * @param op,
     *            the operator in consideration
     * @return true if the returning expression after insert/upsert is invalid; false otherwise.
     */
    public static boolean check(ILogicalOperator op) {
        return checkTopDown(op, false);
    }

    // Checks the query plan rooted at <code>op</code> top down to see whether there is an invalid returning expression
    // for insert/upsert, i.e., a returning expression that contains dataset accesses.
    private static boolean checkTopDown(ILogicalOperator op, boolean hasSubplanAboveWithDatasetAccess) {
        boolean metSubplanWithDataScan = hasSubplanAboveWithDatasetAccess;
        if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
            SubplanOperator subplanOp = (SubplanOperator) op;
            metSubplanWithDataScan = containsDatasetAccess(subplanOp);
        }
        if (op.getOperatorTag() == LogicalOperatorTag.INSERT_DELETE_UPSERT && metSubplanWithDataScan) {
            return true;
        }
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            if (checkTopDown(inputOpRef.getValue(), metSubplanWithDataScan)) {
                return true;
            }
        }
        return false;
    }

    // Checks whether a subplan operator contains a dataset accesses in its nested pipeline.
    private static boolean containsDatasetAccess(SubplanOperator subplanOp) {
        List<ILogicalPlan> nestedPlans = subplanOp.getNestedPlans();
        for (ILogicalPlan nestedPlan : nestedPlans) {
            for (Mutable<ILogicalOperator> opRef : nestedPlan.getRoots()) {
                if (containsDatasetAccessInternal(opRef.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }

    // Checks whether a query plan rooted at <code>op</code> contains dataset accesses.
    private static boolean containsDatasetAccessInternal(ILogicalOperator op) {
        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST) {
            UnnestOperator unnestOp = (UnnestOperator) op;
            ILogicalExpression unnestExpr = unnestOp.getExpressionRef().getValue();
            UnnestingFunctionCallExpression unnestingFuncExpr = (UnnestingFunctionCallExpression) unnestExpr;
            return unnestingFuncExpr.getFunctionIdentifier().equals(BuiltinFunctions.DATASET);
        }
        if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN && containsDatasetAccess((SubplanOperator) op)) {
            return true;
        }
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            if (containsDatasetAccessInternal(childRef.getValue())) {
                return true;
            }
        }
        return false;
    }
}
