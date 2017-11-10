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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexDiskComponent;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexFileManager;

public class LSMInvertedIndexIOOperationCallback extends AbstractLSMIOOperationCallback {

    public LSMInvertedIndexIOOperationCallback(ILSMIndex index, ILSMComponentIdGenerator idGenerator) {
        super(index, idGenerator);
    }

    @Override
    public long getComponentFileLSNOffset(ILSMDiskComponent diskComponent, String diskComponentFilePath)
            throws HyracksDataException {
        if (diskComponentFilePath.endsWith(LSMInvertedIndexFileManager.DELETED_KEYS_BTREE_SUFFIX)) {
            LSMInvertedIndexDiskComponent invIndexComponent = (LSMInvertedIndexDiskComponent) diskComponent;
            IMetadataPageManager metadataPageManager =
                    (IMetadataPageManager) invIndexComponent.getBuddyIndex().getPageManager();
            return metadataPageManager.getFileOffset(metadataPageManager.createMetadataFrame(), LSN_KEY);
        }
        return INVALID;
    }
}
