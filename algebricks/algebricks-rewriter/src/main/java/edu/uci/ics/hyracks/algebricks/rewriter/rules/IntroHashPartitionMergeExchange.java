/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.HashPartitionExchangePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.HashPartitionMergeExchangePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.SortMergeExchangePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroHashPartitionMergeExchange implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getPhysicalOperator() == null
                || op1.getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.HASH_PARTITION_EXCHANGE) {
            return false;
        }
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getPhysicalOperator() == null
                || op2.getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.SORT_MERGE_EXCHANGE) {
            return false;
        }
        HashPartitionExchangePOperator hpe = (HashPartitionExchangePOperator) op1.getPhysicalOperator();
        SortMergeExchangePOperator sme = (SortMergeExchangePOperator) op2.getPhysicalOperator();
        List<OrderColumn> ocList = new ArrayList<OrderColumn>();
        for (OrderColumn oc : sme.getSortColumns()) {
            ocList.add(oc);
        }
        HashPartitionMergeExchangePOperator hpme = new HashPartitionMergeExchangePOperator(ocList, hpe.getHashFields(),
                hpe.getDomain());
        op1.setPhysicalOperator(hpme);
        op1.getInputs().get(0).setValue(op2.getInputs().get(0).getValue());
        op1.computeDeliveredPhysicalProperties(context);
        return true;
    }

}
