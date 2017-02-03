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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeDiskComponent;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeFileManager;

public class LSMRTreeIOOperationCallback extends AbstractLSMIOOperationCallback {

    public LSMRTreeIOOperationCallback() {
        super();
    }

    @Override
    public void afterOperation(LSMOperationType opType, List<ILSMComponent> oldComponents,
            ILSMDiskComponent newComponent) throws HyracksDataException {
        if (newComponent != null) {
            LSMRTreeDiskComponent rtreeComponent = (LSMRTreeDiskComponent) newComponent;
            putLSNIntoMetadata(rtreeComponent, oldComponents);
        }
    }

    @Override
    public long getComponentLSN(List<? extends ILSMComponent> diskComponents) throws HyracksDataException {
        if (diskComponents == null) {
            // Implies a flush IO operation.
            synchronized (this) {
                long lsn = mutableLastLSNs[readIndex];
                return lsn;
            }
        }
        // Get max LSN from the diskComponents. Implies a merge IO operation or Recovery operation.
        long maxLSN = INVALID;
        for (Object o : diskComponents) {
            LSMRTreeDiskComponent rtreeComponent = (LSMRTreeDiskComponent) o;
            maxLSN = Math.max(AbstractLSMIOOperationCallback.getTreeIndexLSN(rtreeComponent.getRTree()), maxLSN);
        }
        return maxLSN;
    }

    @Override
    public long getComponentFileLSNOffset(ILSMDiskComponent diskComponent, String diskComponentFilePath)
            throws HyracksDataException {
        if (diskComponentFilePath.endsWith(LSMRTreeFileManager.RTREE_STRING)) {
            LSMRTreeDiskComponent rtreeComponent = (LSMRTreeDiskComponent) diskComponent;
            IMetadataPageManager metadataPageManager =
                    (IMetadataPageManager) rtreeComponent.getRTree().getPageManager();
            return metadataPageManager.getFileOffset(metadataPageManager.createMetadataFrame(), LSN_KEY);
        }
        return INVALID;
    }
}
