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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

/**
 * Helper class for reading and writing job-gen parameters for vector index access methods.
 *
 * Passes information from the optimizer (Algebricks layer) to the physical runtime
 * (Hyracks layer) for vector index ANN search.
 *
 * Parameters passed via queryVarList (variables assigned in an ASSIGN above the UNNEST-MAP):
 *   [0] = query vector            (e.g., from constant [1.0, 2.0, ...])
 *   [1] = k value                 (from LIMIT k)
 *   [2] = distance metric         (e.g., "euclidean")
 *   [3] = min_probe_fraction      (ANN_DISTANCE arg 3, default 0.1)
 *   [4] = k_multiplier            (ANN_DISTANCE arg 4, default 1)
 */
public class VectorJobGenParams extends AccessMethodJobGenParams {

    protected List<LogicalVariable> queryVarList;

    /**
     * Index-only flag for ANN top-K queries.
     *
     * When {@code true}, the runtime emits a per-candidate distance field alongside the primary
     * key(s), and the optimizer skips the downstream primary BTree lookup + rerank ASSIGN. Used when
     * the projection above {@code LIMIT k → ORDER BY ann_distance(...)} references only PK columns.
     * When {@code false} (the default), the legacy lookup-and-rerank plan is produced and the runtime
     * emits only primary key(s).
     */
    protected boolean indexOnly;

    public VectorJobGenParams() {
    }

    public VectorJobGenParams(String indexName, IndexType indexType, String databaseName, DataverseName dataverseName,
            String datasetName, boolean retainInput, boolean requiresBroadcast) {
        super(indexName, indexType, databaseName, dataverseName, datasetName, retainInput, requiresBroadcast);
    }

    @Override
    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.writeToFuncArgs(funcArgs);
        // indexOnly slot lives between the base params and the variable list — readers can locate it
        // positionally at funcArgs[super.getNumParams()] without knowing the varlist length.
        funcArgs.add(new MutableObject<>(AccessMethodUtils.createBooleanConstant(indexOnly)));
        writeVarList(queryVarList, funcArgs);
    }

    @Override
    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) throws AlgebricksException {
        super.readFromFuncArgs(funcArgs);
        int index = super.getNumParams();
        indexOnly = AccessMethodUtils.getBooleanConstant(funcArgs.get(index));
        index++;
        queryVarList = new ArrayList<>();
        readVarList(funcArgs, index, queryVarList);
    }

    public void setQueryVarList(List<LogicalVariable> queryVarList) {
        this.queryVarList = queryVarList;
    }

    public List<LogicalVariable> getQueryVarList() {
        return queryVarList;
    }

    public void setIndexOnly(boolean indexOnly) {
        this.indexOnly = indexOnly;
    }

    public boolean isIndexOnly() {
        return indexOnly;
    }
}