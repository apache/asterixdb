
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
package org.apache.asterix.optimizer.rules.cbo;

import java.util.List;

import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.optimizer.cost.ICost;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;

public class ScanPlanNode extends AbstractPlanNode {
    private ILogicalOperator leafInput;
    private String datasetName;

    public enum ScanMethod {
        INDEX_SCAN,
        TABLE_SCAN
    }

    private ScanMethod scanOp;
    private ICost opCost;
    private ICost totalCost;
    private Index indexUsed;
    private boolean indexHint;

    public ScanPlanNode(String datasetName, ILogicalOperator leafInput, JoinNode joinNode) {
        super(joinNode);
        this.leafInput = leafInput;
        this.datasetName = datasetName;
    }

    @Override
    public ICost getOpCost() {
        return opCost;
    }

    @Override
    public ICost getTotalCost() {
        return totalCost;
    }

    public ILogicalOperator getLeafInput() {
        return leafInput;
    }

    void setScanMethod(ScanMethod sm) {
        this.scanOp = sm;
    }

    void setScanCosts(ICost opCost) {
        this.opCost = opCost;
        this.totalCost = opCost;
    }

    public void setScanAndHintInfo(ScanMethod scanMethod, List<JoinNode.IndexCostInfo> mandatoryIndexesInfo,
            List<JoinNode.IndexCostInfo> optionalIndexesInfo) {
        setScanMethod(scanMethod);
        if (mandatoryIndexesInfo.size() > 0) {
            indexHint = true;
            numHintsUsed = 1;
        }
        // keeping things simple. When multiple indexes are used, we cannot be sure of the order.
        // So seeing if only index is used.
        if (optionalIndexesInfo.size() + mandatoryIndexesInfo.size() == 1) {
            if (optionalIndexesInfo.size() == 1) {
                indexUsed = optionalIndexesInfo.get(0).getIndex();
            } else {
                indexUsed = mandatoryIndexesInfo.get(0).getIndex();
            }
        }
    }

    public double computeTotalCost() {
        return totalCost.computeTotalCost();
    }

    public ScanMethod getScanOp() {
        return scanOp;
    }

    public Index getSoleAccessIndex() {
        return indexUsed;
    }

    protected double computeOpCost() {
        return opCost.computeTotalCost();
    }
}
