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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import org.apache.asterix.metadata.declared.AqlDataSource;
import org.apache.asterix.metadata.declared.AqlDataSource.AqlDataSourceType;
import org.apache.asterix.metadata.declared.FeedDataSource;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomPartitionPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceRandomPartitioningFeedComputationRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (!op.getOperatorTag().equals(LogicalOperatorTag.ASSIGN)) {
            return false;
        }

        ILogicalOperator opChild = op.getInputs().get(0).getValue();
        if (!opChild.getOperatorTag().equals(LogicalOperatorTag.DATASOURCESCAN)) {
            return false;
        }

        DataSourceScanOperator scanOp = (DataSourceScanOperator) opChild;
        AqlDataSource dataSource = (AqlDataSource) scanOp.getDataSource();
        if (!dataSource.getDatasourceType().equals(AqlDataSourceType.FEED)) {
            return false;
        }

        final FeedDataSource feedDataSource = (FeedDataSource) dataSource;
        Feed feed = feedDataSource.getFeed();
        if (feed.getAppliedFunction() == null) {
            return false;
        }

        ExchangeOperator exchangeOp = new ExchangeOperator();
        INodeDomain domain = new INodeDomain() {
            @Override
            public boolean sameAs(INodeDomain domain) {
                return domain == this;
            }

            @Override
            public Integer cardinality() {
                return feedDataSource.getComputeCardinality();
            }
        };

        exchangeOp.setPhysicalOperator(new RandomPartitionPOperator(domain));
        op.getInputs().get(0).setValue(exchangeOp);
        exchangeOp.getInputs().add(new MutableObject<ILogicalOperator>(scanOp));
        ExecutionMode em = ((AbstractLogicalOperator) scanOp).getExecutionMode();
        exchangeOp.setExecutionMode(em);
        exchangeOp.computeDeliveredPhysicalProperties(context);
        context.computeAndSetTypeEnvironmentForOperator(exchangeOp);

        AssignOperator assignOp = (AssignOperator) opRef.getValue();
        AssignPOperator assignPhyOp = (AssignPOperator) assignOp.getPhysicalOperator();
        assignPhyOp.setCardinalityConstraint(domain.cardinality());

        return true;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

}
