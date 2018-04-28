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
package org.apache.asterix.common.context;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;

public class BaseOperationTracker implements ITransactionOperationTracker {

    protected final int datasetID;
    protected final DatasetInfo dsInfo;

    public BaseOperationTracker(int datasetID, DatasetInfo dsInfo) {
        this.datasetID = datasetID;
        this.dsInfo = dsInfo;
    }

    @Override
    public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        if (opType == LSMOperationType.REPLICATE) {
            dsInfo.declareActiveIOOperation();
        }
    }

    @Override
    public void afterOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        if (opType == LSMOperationType.REPLICATE) {
            completeOperation(index, opType, searchCallback, modificationCallback);
        }
    }

    @Override
    public void completeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        if (opType == LSMOperationType.REPLICATE) {
            dsInfo.undeclareActiveIOOperation();
        }
    }

    @Override
    public void beforeTransaction(long resourceId) {
        /*
         * Increment dataset and index ref count to prevent them
         * from being evicted/dropped until the transaction completes
         */
        dsInfo.touch();
        dsInfo.getIndexes().get(resourceId).touch();
    }

    @Override
    public void afterTransaction(long resourceId) {
        dsInfo.untouch();
        dsInfo.getIndexes().get(resourceId).untouch();
    }

    public DatasetInfo getDatasetInfo() {
        return dsInfo;
    }
}
