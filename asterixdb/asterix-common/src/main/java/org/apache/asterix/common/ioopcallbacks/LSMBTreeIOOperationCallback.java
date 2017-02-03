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
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeDiskComponent;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.utils.ComponentMetadataUtil;

public class LSMBTreeIOOperationCallback extends AbstractLSMIOOperationCallback {

    public LSMBTreeIOOperationCallback() {
        super();
    }

    @Override
    public void afterOperation(LSMOperationType opType, List<ILSMComponent> oldComponents,
            ILSMDiskComponent newComponent) throws HyracksDataException {
        //TODO: Copying Filters and all content of the metadata pages for flush operation should be done here
        if (newComponent != null) {
            LSMBTreeDiskComponent btreeComponent = (LSMBTreeDiskComponent) newComponent;
            putLSNIntoMetadata(btreeComponent, oldComponents);
            if (opType == LSMOperationType.MERGE) {
                LongPointable markerLsn = LongPointable.FACTORY
                        .createPointable(ComponentMetadataUtil.getLong(oldComponents.get(0).getMetadata(),
                                ComponentMetadataUtil.MARKER_LSN_KEY, ComponentMetadataUtil.NOT_FOUND));
                btreeComponent.getMetadata().put(ComponentMetadataUtil.MARKER_LSN_KEY, markerLsn);
            }

        }
    }

    @Override
    public long getComponentLSN(List<? extends ILSMComponent> diskComponents) throws HyracksDataException {
        if (diskComponents == null) {
            // Implies a flush IO operation. --> moves the flush pointer
            // Flush operation of an LSM index are executed sequentially.
            synchronized (this) {
                long lsn = mutableLastLSNs[readIndex];
                return lsn;
            }
        }
        // Get max LSN from the diskComponents. Implies a merge IO operation or Recovery operation.
        long maxLSN = -1L;
        for (ILSMComponent c : diskComponents) {
            BTree btree = ((LSMBTreeDiskComponent) c).getBTree();
            maxLSN = Math.max(AbstractLSMIOOperationCallback.getTreeIndexLSN(btree), maxLSN);
        }
        return maxLSN;
    }

    @Override
    public long getComponentFileLSNOffset(ILSMDiskComponent diskComponent, String diskComponentFilePath)
            throws HyracksDataException {
        if (diskComponentFilePath.endsWith(LSMBTreeFileManager.BTREE_STRING)) {
            LSMBTreeDiskComponent btreeComponent = (LSMBTreeDiskComponent) diskComponent;
            IMetadataPageManager metadataPageManager =
                    (IMetadataPageManager) btreeComponent.getBTree().getPageManager();
            return metadataPageManager.getFileOffset(metadataPageManager.createMetadataFrame(), LSN_KEY);
        }
        return INVALID;
    }
}
