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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.ColumnAwareDiskOnlyMultiComparator;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LSMColumnBTreeRangeSearchCursor extends LSMBTreeRangeSearchCursor {
    private static final Logger LOGGER = LogManager.getLogger();
    private final List<IColumnTupleIterator> componentTupleList;

    public LSMColumnBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false, NoOpIndexCursorStats.INSTANCE);
    }

    public LSMColumnBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples,
            IIndexCursorStats stats) {
        super(opCtx, returnDeletedTuples, stats);
        componentTupleList = new ArrayList<>();
    }

    @Override
    protected BTreeAccessor createAccessor(ILSMComponent component, int index) throws HyracksDataException {
        if (component.getType() == LSMComponentType.MEMORY) {
            return super.createAccessor(component, index);
        }

        ColumnBTree columnBTree = (ColumnBTree) component.getIndex();
        LSMColumnBTreeOpContext columnOpCtx = (LSMColumnBTreeOpContext) opCtx;
        IColumnProjectionInfo projectionInfo = columnOpCtx.createProjectionInfo();
        IColumnReadContext context = columnOpCtx.createPageZeroContext(projectionInfo);
        return columnBTree.createAccessor(NoOpIndexAccessParameters.INSTANCE, index, projectionInfo, context);
    }

    @Override
    protected IIndexCursor createCursor(LSMComponentType type, BTreeAccessor accessor) {
        if (type == LSMComponentType.MEMORY) {
            return super.createCursor(type, accessor);
        }
        ColumnBTreeRangeSearchCursor cursor = (ColumnBTreeRangeSearchCursor) accessor.createSearchCursor(false);
        componentTupleList.add((IColumnTupleIterator) cursor.doGetTuple());
        return cursor;
    }

    @Override
    protected void markAsDeleted(PriorityQueueElement e) throws HyracksDataException {
        if (isMemoryComponent[e.getCursorIndex()]) {
            super.markAsDeleted(e);
            return;
        }
        IColumnTupleIterator columnTuple = (IColumnTupleIterator) e.getTuple();
        if (!columnTuple.isAntimatter()) {
            // Skip non-key columns
            columnTuple.skip(1);
        }
    }

    @Override
    protected void setPriorityQueueComparator() {
        if (!includeMutableComponent) {
            cmp = new ColumnAwareDiskOnlyMultiComparator(cmp);
        }
        if (pqCmp == null || cmp != pqCmp.getMultiComparator()) {
            pqCmp = new PriorityQueueComparator(cmp);
        }
    }

    @Override
    protected void excludeMemoryComponent() {
        //Replace the comparator with disk only comparator
        pqCmp.setMultiComparator(new ColumnAwareDiskOnlyMultiComparator(cmp));
    }

    @Override
    protected int replaceFrom() {
        //Disable replacing the in-memory component to disk component as the schema may change
        //TODO at least allow the replacement when no schema changes occur
        return -1;
    }

    /**
     * @return we need the tuple references for vertical merges
     */
    public List<IColumnTupleIterator> getComponentTupleList() {
        return componentTupleList;
    }
}
