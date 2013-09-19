/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.common.ioopcallbacks;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTreeDiskComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;

public class LSMBTreeIOOperationCallback extends AbstractLSMIOOperationCallback {

    public LSMBTreeIOOperationCallback() {
        super();
    }

    @Override
    public void afterOperation(LSMOperationType opType, List<ILSMComponent> oldComponents, ILSMComponent newComponent)
            throws HyracksDataException {
        if (newComponent != null) {
            LSMBTreeDiskComponent btreeComponent = (LSMBTreeDiskComponent) newComponent;
            putLSNIntoMetadata(btreeComponent.getBTree(), oldComponents);
        }
    }

    @Override
    public long getComponentLSN(List<ILSMComponent> diskComponents) throws HyracksDataException {
        if (diskComponents == null) {
            // Implies a flush IO operation.
            synchronized (this) {
                long lsn = immutableLastLSNs[readIndex];
                readIndex = (readIndex + 1) % immutableLastLSNs.length;
                return lsn;
            }
        }
        // Get max LSN from the diskComponents. Implies a merge IO operation or Recovery operation.
        long maxLSN = -1;
        for (ILSMComponent c : diskComponents) {
            BTree btree = ((LSMBTreeDiskComponent) c).getBTree();
            maxLSN = Math.max(getTreeIndexLSN(btree), maxLSN);
        }
        return maxLSN;
    }
}
