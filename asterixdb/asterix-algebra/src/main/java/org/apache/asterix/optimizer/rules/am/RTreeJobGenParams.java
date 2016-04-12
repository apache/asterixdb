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
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

/**
 * Helper class for reading and writing job-gen parameters for RTree access methods to
 * and from a list of function arguments, typically of an unnest-map.
 */
public class RTreeJobGenParams extends AccessMethodJobGenParams {

    protected List<LogicalVariable> keyVarList;

    public RTreeJobGenParams() {
    }

    public RTreeJobGenParams(String indexName, IndexType indexType, String dataverseName, String datasetName,
            boolean retainInput, boolean requiresBroadcast) {
        super(indexName, indexType, dataverseName, datasetName, retainInput, requiresBroadcast);
    }

    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.writeToFuncArgs(funcArgs);
        writeVarList(keyVarList, funcArgs);
    }

    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.readFromFuncArgs(funcArgs);
        int index = super.getNumParams();
        keyVarList = new ArrayList<LogicalVariable>();
        readVarList(funcArgs, index, keyVarList);
    }

    public void setKeyVarList(List<LogicalVariable> keyVarList) {
        this.keyVarList = keyVarList;
    }

    public List<LogicalVariable> getKeyVarList() {
        return keyVarList;
    }
}
