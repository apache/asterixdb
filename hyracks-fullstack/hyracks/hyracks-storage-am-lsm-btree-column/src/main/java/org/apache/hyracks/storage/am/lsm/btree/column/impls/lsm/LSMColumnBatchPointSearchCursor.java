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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class LSMColumnBatchPointSearchCursor extends LSMBTreeBatchPointSearchCursor {

    public LSMColumnBatchPointSearchCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx);
    }

    @Override
    protected BTreeAccessor createAccessor(LSMComponentType type, BTree btree, int index) throws HyracksDataException {
        if (type == LSMComponentType.MEMORY) {
            return super.createAccessor(type, btree, index);
        }
        ColumnBTree columnBTree = (ColumnBTree) btree;
        LSMColumnBTreeOpContext columnOpCtx = (LSMColumnBTreeOpContext) opCtx;
        IColumnProjectionInfo projectionInfo = columnOpCtx.createProjectionInfo();
        IColumnReadContext context = columnOpCtx.createPageZeroContext(projectionInfo);
        return columnBTree.createAccessor(NoOpIndexAccessParameters.INSTANCE, index, projectionInfo, context);
    }
}
