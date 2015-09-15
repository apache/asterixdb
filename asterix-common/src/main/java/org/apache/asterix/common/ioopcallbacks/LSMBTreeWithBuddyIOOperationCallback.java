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
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeWithBuddyDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMOperationType;

public class LSMBTreeWithBuddyIOOperationCallback extends AbstractLSMIOOperationCallback {

    @Override
    public void afterOperation(LSMOperationType opType, List<ILSMComponent> oldComponents, ILSMComponent newComponent)
            throws HyracksDataException {
        if (newComponent != null) {
            LSMBTreeWithBuddyDiskComponent btreeComponent = (LSMBTreeWithBuddyDiskComponent) newComponent;
            putLSNIntoMetadata(btreeComponent.getBTree(), oldComponents);
            putLSNIntoMetadata(btreeComponent.getBuddyBTree(), oldComponents);
        }
    }

    @Override
    public long getComponentLSN(List<ILSMComponent> diskComponents) throws HyracksDataException {
        if (diskComponents == null) {
            // Implies a flush IO operation <Will never happen currently as Btree with buddy btree is only used with external datasets>
            synchronized (this) {
                long lsn = mutableLastLSNs[readIndex];
                return lsn;
            }
        }
        // Get max LSN from the diskComponents. Implies a merge IO operation or Recovery operation.
        long maxLSN = -1;
        for (Object o : diskComponents) {
            LSMBTreeWithBuddyDiskComponent btreeComponent = (LSMBTreeWithBuddyDiskComponent) o;
            maxLSN = Math.max(getTreeIndexLSN(btreeComponent.getBTree()), maxLSN);
        }
        return maxLSN;
    }

}
