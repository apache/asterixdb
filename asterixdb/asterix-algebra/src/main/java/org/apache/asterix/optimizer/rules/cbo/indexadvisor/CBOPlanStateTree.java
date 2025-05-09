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

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;

public class CBOPlanStateTree {
    private final AbstractAdvisorPlanNode cboPlanNode;
    private final Map<LogicalVariable, DataSourceScanOperator> dataSourceScanVariableMap;

    public CBOPlanStateTree(AbstractAdvisorPlanNode cboPlanNode) {
        this.cboPlanNode = cboPlanNode;
        dataSourceScanVariableMap = new HashMap<>();
        for (AdvisorScanPlanNode scanNode : cboPlanNode.getLeafs()) {
            DataSourceScanOperator scanOperator = scanNode.getScanOperator();
            for (LogicalVariable variable : scanOperator.getVariables()) {
                dataSourceScanVariableMap.put(variable, scanOperator);
            }
        }
    }

    public AbstractAdvisorPlanNode getCboPlanNode() {
        return cboPlanNode;
    }

    public Map<LogicalVariable, DataSourceScanOperator> getDataSourceScanVariableMap() {
        return dataSourceScanVariableMap;
    }

}
