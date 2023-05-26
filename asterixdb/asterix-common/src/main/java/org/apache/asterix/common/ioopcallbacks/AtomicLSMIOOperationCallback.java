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

package org.apache.asterix.common.ioopcallbacks;

import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;

public class AtomicLSMIOOperationCallback extends LSMIOOperationCallback {

    public AtomicLSMIOOperationCallback(DatasetInfo dsInfo, ILSMIndex lsmIndex, ILSMComponentId componentId,
            IIndexCheckpointManagerProvider indexCheckpointManagerProvider) {
        super(dsInfo, lsmIndex, componentId, indexCheckpointManagerProvider);
    }

    @Override
    public void afterFinalize(ILSMIOOperation operation) throws HyracksDataException {
        if (operation.getStatus() == LSMIOOperationStatus.FAILURE) {
            return;
        }
        if (operation.getIOOpertionType() != LSMIOOperationType.LOAD
                && operation.getAccessor().getOpContext().getOperation() == IndexOperation.DELETE_COMPONENTS) {
            deleteComponentsFromCheckpoint(operation);
        } else if (operation.getIOOpertionType() == LSMIOOperationType.LOAD) {
            addComponentToCheckpoint(operation);
        } else if (isMerge(operation)) {
            IoUtil.delete(getOperationMaskFilePath(operation));
        }
    }
}
