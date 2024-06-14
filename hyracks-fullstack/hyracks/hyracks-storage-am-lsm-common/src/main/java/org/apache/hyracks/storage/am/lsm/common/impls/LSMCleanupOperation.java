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

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LSMCleanupOperation extends AbstractIoOperation {

    private static final Logger LOGGER = LogManager.getLogger();
    private final List<ILSMDiskComponent> inactiveDiskComponents;

    public LSMCleanupOperation(ILSMIndex index, ILSMIOOperationCallback ioOpCallback,
            List<ILSMDiskComponent> inactiveDiskComponents) {
        super(null, null, ioOpCallback, index.getIndexIdentifier());
        this.inactiveDiskComponents = inactiveDiskComponents;
    }

    @Override
    public ILSMIOOperation.LSMIOOperationType getIOOperationType() {
        return LSMIOOperationType.CLEANUP;
    }

    @Override
    public ILSMIOOperation.LSMIOOperationStatus call() throws HyracksDataException {
        try {
            LOGGER.debug("started cleanup operation on index {} to destroy components {}", getIndexIdentifier(),
                    inactiveDiskComponents);
            for (ILSMDiskComponent diskComponent : inactiveDiskComponents) {
                diskComponent.deactivateAndDestroy();
            }
            LOGGER.debug("completed cleanup operation on index {} to destroy components {}", getIndexIdentifier(),
                    inactiveDiskComponents);
            return ILSMIOOperation.LSMIOOperationStatus.SUCCESS;
        } catch (Exception e) {
            setFailure(e);
            return LSMIOOperationStatus.FAILURE;
        }
    }

    @Override
    public long getRemainingPages() {
        return 0;
    }

    @Override
    protected LSMComponentFileReferences getComponentFiles() {
        return null;
    }
}
