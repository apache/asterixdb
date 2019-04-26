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

package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;

public abstract class AbstractDistinctByPOperator extends AbstractPhysicalOperator {

    protected List<LogicalVariable> columnList;

    protected AbstractDistinctByPOperator(List<LogicalVariable> columnList) {
        this.columnList = columnList;
    }

    public List<LogicalVariable> getDistinctByColumns() {
        return columnList;
    }

    public void setDistinctByColumns(List<LogicalVariable> distinctByColumns) {
        this.columnList = distinctByColumns;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }

    protected int[] getKeysAndDecs(IOperatorSchema inputSchema) {
        int keys[] = JobGenHelper.variablesToFieldIndexes(columnList, inputSchema);
        int sz = inputSchema.getSize();
        int fdSz = sz - columnList.size();
        int[] fdColumns = new int[fdSz];
        int j = 0;
        for (LogicalVariable v : inputSchema) {
            if (!columnList.contains(v)) {
                fdColumns[j++] = inputSchema.findVariable(v);
            }
        }
        int[] keysAndDecs = new int[keys.length + fdColumns.length];
        for (int i = 0; i < keys.length; i++) {
            keysAndDecs[i] = keys[i];
        }
        for (int i = 0; i < fdColumns.length; i++) {
            keysAndDecs[i + keys.length] = fdColumns[i];
        }
        return keysAndDecs;
    }
}