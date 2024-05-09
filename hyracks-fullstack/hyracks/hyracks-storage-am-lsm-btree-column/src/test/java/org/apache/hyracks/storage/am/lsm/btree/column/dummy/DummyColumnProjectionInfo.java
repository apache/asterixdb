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
package org.apache.hyracks.storage.am.lsm.btree.column.dummy;

import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;

import it.unimi.dsi.fastutil.ints.IntList;

public class DummyColumnProjectionInfo implements IColumnProjectionInfo {
    private final int numberOfPrimaryKeys;
    private final ColumnProjectorType projectorType;
    private final IntList projectedColumns;

    public DummyColumnProjectionInfo(int numberOfPrimaryKeys, ColumnProjectorType projectorType,
            IntList projectedColumns) {
        this.numberOfPrimaryKeys = numberOfPrimaryKeys;
        this.projectorType = projectorType;
        this.projectedColumns = projectedColumns;
    }

    @Override
    public int getColumnIndex(int ordinal) {
        return projectedColumns.getInt(ordinal);
    }

    @Override
    public int getNumberOfProjectedColumns() {
        return projectedColumns.size();
    }

    @Override
    public int getNumberOfPrimaryKeys() {
        return numberOfPrimaryKeys;
    }

    @Override
    public int getFilteredColumnIndex(int ordinal) {
        return -1;
    }

    @Override
    public int getNumberOfFilteredColumns() {
        return 0;
    }

    @Override
    public ColumnProjectorType getProjectorType() {
        return projectorType;
    }

    @Override
    public String toString() {
        return projectedColumns.toString();
    }
}
