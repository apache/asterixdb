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
package org.apache.asterix.optimizer.rules.cbo.indexadvisor;

import java.util.Collections;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;

public class AdvisorScanPlanNode extends AbstractAdvisorPlanNode {
    private ILogicalOperator op;
    private final DataSourceScanOperator scanOperator;
    private final ScanFilter filter;

    public AdvisorScanPlanNode(ILogicalOperator op, IOptimizationContext context) throws AlgebricksException {
        super(PlanNodeType.SCAN);
        this.op = op;
        this.scanOperator = findDatascanOperator(op);
        filter = AdvisorConditionParser.parseScanNode(op, context);
    }

    private DataSourceScanOperator findDatascanOperator(ILogicalOperator op) {
        while (op.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            op = op.getInputs().get(0).getValue();
        }
        return (DataSourceScanOperator) op;
    }

    public DataSourceScanOperator getScanOperator() {
        return scanOperator;
    }

    @Override
    public List<AdvisorScanPlanNode> getLeafs() {
        return Collections.singletonList(this);
    }

    @Override
    public List<AdvisorJoinPlanNode> getJoins() {
        return Collections.emptyList();
    }

    public ScanFilter getFilter() {
        return filter;
    }

}
