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
package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.optimizer.base.AnalysisUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;

/**
 * Operator subtree that matches the following patterns, and provides convenient access to its nodes:
 * (select)? <-- (assign)+ <-- (datasource scan)
 * and
 * (select)? <-- (datasource scan)
 */
public class OptimizableOperatorSubTree {
    public ILogicalOperator root = null;
    public Mutable<ILogicalOperator> rootRef = null;
    public final List<Mutable<ILogicalOperator>> assignRefs = new ArrayList<Mutable<ILogicalOperator>>();
    public final List<AssignOperator> assigns = new ArrayList<AssignOperator>();
    public Mutable<ILogicalOperator> dataSourceScanRef = null;
    public DataSourceScanOperator dataSourceScan = null;
    // Dataset and type metadata. Set in setDatasetAndTypeMetadata().
    public Dataset dataset = null;
    public ARecordType recordType = null;

    public boolean initFromSubTree(Mutable<ILogicalOperator> subTreeOpRef) {
        reset();
        rootRef = subTreeOpRef;
        root = subTreeOpRef.getValue();
        // Examine the op's children to match the expected patterns.
        AbstractLogicalOperator subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
        // Skip select operator.
        if (subTreeOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
            subTreeOpRef = subTreeOp.getInputs().get(0);
            subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
        }
        // Check primary-index pattern.
        if (subTreeOp.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            // Pattern may still match if we are looking for primary index matches as well.
            if (subTreeOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                dataSourceScanRef = subTreeOpRef;
                dataSourceScan = (DataSourceScanOperator) subTreeOp;
                return true;
            }
            return false;
        }
        // Match (assign)+.
        do {
            assignRefs.add(subTreeOpRef);
            assigns.add((AssignOperator) subTreeOp);
            subTreeOpRef = subTreeOp.getInputs().get(0);
            subTreeOp = (AbstractLogicalOperator) subTreeOpRef.getValue();
        } while (subTreeOp.getOperatorTag() == LogicalOperatorTag.ASSIGN);
        // Set to last valid assigns.
        subTreeOpRef = assignRefs.get(assignRefs.size() - 1);
        subTreeOp = assigns.get(assigns.size() - 1);
        // Match datasource scan.
        Mutable<ILogicalOperator> opRef3 = subTreeOp.getInputs().get(0);
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) opRef3.getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }
        dataSourceScanRef = opRef3;
        dataSourceScan = (DataSourceScanOperator) op3;
        return true;
    }

    /**
     * Find the dataset corresponding to the datasource scan in the metadata.
     * Also sets recordType to be the type of that dataset.
     */
    public boolean setDatasetAndTypeMetadata(AqlMetadataProvider metadataProvider) throws AlgebricksException {
        if (dataSourceScan == null) {
            return false;
        }
        // Find the dataset corresponding to the datasource scan in the metadata.
        Pair<String, String> datasetInfo = AnalysisUtil.getDatasetInfo(dataSourceScan);
        String dataverseName = datasetInfo.first;
        String datasetName = datasetInfo.second;
        if (dataverseName == null || datasetName == null) {
            return false;
        }
        dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("No metadata for dataset " + datasetName);
        }
        if (dataset.getDatasetType() != DatasetType.INTERNAL && dataset.getDatasetType() != DatasetType.FEED) {
            return false;
        }
        // Get the record type for that dataset.
        IAType itemType = metadataProvider.findType(dataverseName, dataset.getItemTypeName());
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            return false;
        }
        recordType = (ARecordType) itemType;
        return true;
    }

    public boolean hasDataSourceScan() {
        return dataSourceScan != null;
    }
    
    public void reset() {
        root = null;
        rootRef = null;
        assignRefs.clear();
        assigns.clear();
        dataSourceScanRef = null;
        dataSourceScan = null;
        dataset = null;
        recordType = null;
    }
    
    public void getPrimaryKeyVars(List<LogicalVariable> target) {
        int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        for (int i = 0; i < numPrimaryKeys; i++) {
            target.add(dataSourceScan.getVariables().get(i));
        }
    }
}
