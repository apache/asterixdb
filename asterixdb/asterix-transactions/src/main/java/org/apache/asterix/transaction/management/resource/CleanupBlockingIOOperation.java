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
package org.apache.asterix.transaction.management.resource;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.common.api.IIOBlockingOperation;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;

class CleanupBlockingIOOperation implements IIOBlockingOperation {
    private final int partition;
    private final PersistentLocalResourceRepository localRepository;
    private final IIOManager ioManager;
    private final Set<FileReference> cleanedIndexes;

    public CleanupBlockingIOOperation(int partition, PersistentLocalResourceRepository localRepository,
            IIOManager ioManager) {
        this.partition = partition;
        this.localRepository = localRepository;
        this.ioManager = ioManager;
        cleanedIndexes = new HashSet<>();
    }

    @Override
    public void beforeOperation() throws HyracksDataException {
        // NoOp
    }

    /**
     * Clean all active indexes while the DatasetInfo is synchronized
     *
     * @param indexes active indexes to clean
     */
    @Override
    public void perform(Collection<IndexInfo> indexes) throws HyracksDataException {
        for (IndexInfo indexInfo : indexes) {
            FileReference index = ioManager.resolve(indexInfo.getLocalResource().getPath());
            localRepository.cleanupIndex(index);
            cleanedIndexes.add(index);
        }
    }

    /**
     * Clean all inactive indexes while the DatasetLifeCycleManager is synchronized
     */
    @Override
    public void afterOperation() throws HyracksDataException {
        Set<FileReference> indexes = localRepository.getPartitionIndexes(partition);
        for (FileReference index : indexes) {
            if (!cleanedIndexes.contains(index)) {
                localRepository.cleanupIndex(index);
            }
        }
    }

}
