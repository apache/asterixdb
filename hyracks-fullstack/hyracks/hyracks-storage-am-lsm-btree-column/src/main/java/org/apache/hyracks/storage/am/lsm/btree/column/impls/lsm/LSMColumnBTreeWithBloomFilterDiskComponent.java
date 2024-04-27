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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnMetadata;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.IColumnIndexDiskCacheManager;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeMergeOperation;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeWithBloomFilterDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.ChainedLSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.IChainedComponentBulkLoader;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;

public class LSMColumnBTreeWithBloomFilterDiskComponent extends LSMBTreeWithBloomFilterDiskComponent {

    public LSMColumnBTreeWithBloomFilterDiskComponent(AbstractLSMIndex lsmIndex, BTree btree, BloomFilter bloomFilter,
            ILSMComponentFilter filter) {
        super(lsmIndex, btree, bloomFilter, filter);
    }

    @Override
    public ChainedLSMDiskComponentBulkLoader createBulkLoader(ILSMIOOperation operation, float fillFactor,
            boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter,
            boolean cleanupEmptyComponent, IPageWriteCallback callback) throws HyracksDataException {
        ChainedLSMDiskComponentBulkLoader chainedBulkLoader =
                new ChainedLSMDiskComponentBulkLoader(operation, this, cleanupEmptyComponent);
        if (withFilter && getLsmIndex().getFilterFields() != null) {
            //Add filter writer if exists
            chainedBulkLoader.addBulkLoader(createFilterBulkLoader());
        }
        //Add index bulkloader
        chainedBulkLoader.addBulkLoader(createColumnIndexBulkLoader(operation, fillFactor, verifyInput, callback));

        if (numElementsHint > 0) {
            chainedBulkLoader.addBulkLoader(createBloomFilterBulkLoader(numElementsHint, callback));
        }

        callback.initialize(chainedBulkLoader);
        return chainedBulkLoader;
    }

    private IChainedComponentBulkLoader createColumnIndexBulkLoader(ILSMIOOperation operation, float fillFactor,
            boolean verifyInput, IPageWriteCallback callback) throws HyracksDataException {
        LSMIOOperationType operationType = operation.getIOOpertionType();
        LSMColumnBTree lsmColumnBTree = (LSMColumnBTree) getLsmIndex();
        ColumnBTree columnBTree = (ColumnBTree) getIndex();
        IColumnMetadata columnMetadata;
        if (operationType == LSMIOOperationType.FLUSH || operationType == LSMIOOperationType.LOAD) {
            columnMetadata = lsmColumnBTree.getColumnMetadata();
        } else {
            //Merge
            LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
            LSMColumnBTreeRangeSearchCursor cursor = (LSMColumnBTreeRangeSearchCursor) mergeOp.getCursor();
            List<ILSMComponent> mergingComponents = mergeOp.getMergingComponents();
            IComponentMetadata componentMetadata = mergingComponents.get(0).getMetadata();
            IValueReference columnMetadataValue = ColumnUtil.getColumnMetadataCopy(componentMetadata);
            columnMetadata = lsmColumnBTree.getColumnManager().createMergeColumnMetadata(columnMetadataValue,
                    cursor.getComponentTupleList());
        }
        int numberOfColumns = columnMetadata.getNumberOfColumns();
        IColumnIndexDiskCacheManager diskCacheManager = lsmColumnBTree.getDiskCacheManager();
        IColumnWriteContext writeContext = diskCacheManager.createWriteContext(numberOfColumns, operationType);
        IIndexBulkLoader bulkLoader =
                columnBTree.createBulkLoader(fillFactor, verifyInput, callback, columnMetadata, writeContext);
        return new LSMColumnIndexBulkloader(bulkLoader, columnMetadata, getMetadata());
    }
}
