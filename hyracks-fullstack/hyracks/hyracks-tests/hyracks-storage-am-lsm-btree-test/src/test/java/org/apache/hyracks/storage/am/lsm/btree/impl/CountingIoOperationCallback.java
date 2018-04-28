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
package org.apache.hyracks.storage.am.lsm.btree.impl;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;

public class CountingIoOperationCallback implements ILSMIOOperationCallback {
    private int beforeOperation;
    private int afterOperation;
    private int afterFinalize;
    private int recycled;
    private int allocated;
    private int beforeSchedule;
    private int destroy;

    public CountingIoOperationCallback() {
    }

    public int getAfterFinalizeCount() {
        return afterFinalize;
    }

    @Override
    public void recycled(ILSMMemoryComponent component) throws HyracksDataException {
        recycled++;
    }

    public int getRecycledCount() {
        return recycled;
    }

    @Override
    public void allocated(ILSMMemoryComponent component) throws HyracksDataException {
        allocated++;
    }

    public int getAllocatedCount() {
        return allocated;
    }

    @Override
    public void scheduled(ILSMIOOperation operation) throws HyracksDataException {
        beforeSchedule++;
    }

    public int getBeforeScheduleCount() {
        return beforeSchedule;
    }

    @Override
    public void beforeOperation(ILSMIOOperation operation) throws HyracksDataException {
        beforeOperation++;
    }

    public int getBeforeOperationCount() {
        return beforeOperation;
    }

    @Override
    public void afterOperation(ILSMIOOperation operation) throws HyracksDataException {
        afterOperation++;
    }

    public int getAfterOperationCount() {
        return afterOperation;
    }

    @Override
    public void afterFinalize(ILSMIOOperation operation) throws HyracksDataException {
        afterFinalize++;
    }

    @Override
    public void completed(ILSMIOOperation operation) {
        destroy++;
    }

    public int getDestroyCount() {
        return destroy;
    }

}
