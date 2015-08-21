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
package edu.uci.ics.asterix.optimizer.rules;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource.AqlDataSourceType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class RemoveSortInFeedIngestionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE) {
            return false;
        }

        AbstractLogicalOperator insertOp = op;
        AbstractLogicalOperator descendantOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        boolean isSourceAFeed = false;
        while (descendantOp != null) {
            if (descendantOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                AqlDataSource dataSource = (AqlDataSource) ((DataSourceScanOperator) descendantOp).getDataSource();
                if (dataSource.getDatasourceType().equals(AqlDataSourceType.FEED)) {
                    isSourceAFeed = true;
                }
                break;
            }
            if (descendantOp.getInputs().isEmpty()) {
                break;
            }
            descendantOp = (AbstractLogicalOperator) descendantOp.getInputs().get(0).getValue();
        }

        if (isSourceAFeed) {
            AbstractLogicalOperator prevOp = (AbstractLogicalOperator) insertOp.getInputs().get(0).getValue();
            if (prevOp.getOperatorTag() == LogicalOperatorTag.ORDER) {
                insertOp.getInputs().set(0, prevOp.getInputs().get(0));
                return true;
            }
        }

        return false;
    }

}
