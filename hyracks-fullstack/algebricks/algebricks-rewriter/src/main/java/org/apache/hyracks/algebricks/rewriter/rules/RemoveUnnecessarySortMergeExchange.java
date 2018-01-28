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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionMergeExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortMergeExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class RemoveUnnecessarySortMergeExchange implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getPhysicalOperator() == null
                || (op1.getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.HASH_PARTITION_EXCHANGE && op1
                        .getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.HASH_PARTITION_MERGE_EXCHANGE)) {
            return false;
        }
        Mutable<ILogicalOperator> currentOpRef = op1.getInputs().get(0);
        AbstractLogicalOperator currentOp = (AbstractLogicalOperator) currentOpRef.getValue();

        // Goes down the pipeline to find a qualified SortMergeExchange to eliminate.
        while (currentOp != null) {
            IPhysicalOperator physicalOp = currentOp.getPhysicalOperator();
            if (physicalOp == null) {
                return false;
            } else if (physicalOp.getOperatorTag() == PhysicalOperatorTag.SORT_MERGE_EXCHANGE) {
                break;
            } else if (!currentOp.isMap() || currentOp.getOperatorTag() == LogicalOperatorTag.UNNEST
                    || currentOp.getOperatorTag() == LogicalOperatorTag.LIMIT) {
                // Do not eliminate sort-merge below input order-sensitive operators.
                // TODO(buyingyi): once Taewoo merges his limit-push down change,
                // we need to use his new property in logical operator to check order sensitivity.
                return false;
            } else if (currentOp.getInputs().size() == 1) {
                currentOpRef = currentOp.getInputs().get(0);
                currentOp = (AbstractLogicalOperator) currentOpRef.getValue();
            } else {
                currentOp = null;
            }
        }
        if (currentOp == null) {
            // There is no such qualified SortMergeExchange.
            return false;
        }

        if (op1.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.HASH_PARTITION_MERGE_EXCHANGE) {
            // If op1 is a hash_partition_merge_exchange, the sort_merge_exchange can be simply removed.
            currentOpRef.setValue(currentOp.getInputs().get(0).getValue());
            op1.computeDeliveredPhysicalProperties(context);
            return true;
        }

        // Checks whether sort columns in the SortMergeExchange are still available at op1.
        // If yes, we use HashMergeExchange; otherwise, we use HashExchange.
        SortMergeExchangePOperator sme = (SortMergeExchangePOperator) currentOp.getPhysicalOperator();
        HashPartitionExchangePOperator hpe = (HashPartitionExchangePOperator) op1.getPhysicalOperator();
        Set<LogicalVariable> liveVars = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(op1, liveVars);
        boolean usingHashMergeExchange = true;
        for (OrderColumn oc : sme.getSortColumns()) {
            if (!liveVars.contains(oc.getColumn())) {
                usingHashMergeExchange = false;
            }
        }

        if (usingHashMergeExchange) {
            // Add sort columns from the SortMergeExchange into a new HashMergeExchange.
            List<OrderColumn> ocList = new ArrayList<OrderColumn>();
            for (OrderColumn oc : sme.getSortColumns()) {
                ocList.add(oc);
            }
            HashPartitionMergeExchangePOperator hpme =
                    new HashPartitionMergeExchangePOperator(ocList, hpe.getHashFields(), hpe.getDomain());
            op1.setPhysicalOperator(hpme);
        }

        // Remove the SortMergeExchange op.
        currentOpRef.setValue(currentOp.getInputs().get(0).getValue());

        // Re-compute delivered properties at op1.
        op1.computeDeliveredPhysicalProperties(context);
        return true;
    }

}
