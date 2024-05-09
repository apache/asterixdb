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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep;

import java.util.BitSet;

import org.apache.hyracks.cloud.buffercache.page.ISweepLockInfo;

public final class ColumnSweepLockInfo implements ISweepLockInfo {
    private final BitSet lockedColumns;

    public ColumnSweepLockInfo() {
        lockedColumns = new BitSet();
    }

    /**
     * Reset the lock with plan's columns
     *
     * @param plan contains the columns to be locked
     */
    void reset(BitSet plan) {
        lockedColumns.clear();
        lockedColumns.or(plan);
    }

    /**
     * Clear and set the locked columns in the provided {@link BitSet}
     *
     * @param lockedColumns used to get the locked columns
     */
    public void getLockedColumns(BitSet lockedColumns) {
        lockedColumns.clear();
        lockedColumns.or(this.lockedColumns);
    }

    @Override
    public boolean isLocked() {
        return true;
    }
}
