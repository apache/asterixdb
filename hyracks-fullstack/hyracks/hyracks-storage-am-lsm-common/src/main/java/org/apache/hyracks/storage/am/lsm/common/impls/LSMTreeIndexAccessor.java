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
import java.util.function.Predicate;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameTupleProcessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class LSMTreeIndexAccessor implements ILSMIndexAccessor {
    @FunctionalInterface
    public interface ICursorFactory {
        IIndexCursor create(ILSMIndexOperationContext ctx);
    }

    protected final ILSMHarness lsmHarness;
    protected final ILSMIndexOperationContext ctx;
    protected final ICursorFactory cursorFactory;
    private boolean destroyed = false;

    public LSMTreeIndexAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx, ICursorFactory cursorFactory) {
        this.lsmHarness = lsmHarness;
        this.ctx = ctx;
        this.cursorFactory = cursorFactory;
    }

    public ILSMIndexOperationContext getCtx() {
        return ctx;
    }

    @Override
    public void insert(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.INSERT);
        lsmHarness.modify(ctx, false, tuple);
    }

    @Override
    public void update(ITupleReference tuple) throws HyracksDataException {
        // Update is the same as insert.
        ctx.setOperation(IndexOperation.UPDATE);
        lsmHarness.modify(ctx, false, tuple);
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.DELETE);
        lsmHarness.modify(ctx, false, tuple);
    }

    @Override
    public void upsert(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.UPSERT);
        lsmHarness.modify(ctx, false, tuple);
    }

    @Override
    public boolean tryInsert(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.INSERT);
        return lsmHarness.modify(ctx, true, tuple);
    }

    @Override
    public boolean tryDelete(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.DELETE);
        return lsmHarness.modify(ctx, true, tuple);
    }

    @Override
    public boolean tryUpdate(ITupleReference tuple) throws HyracksDataException {
        // Update is the same as insert.
        ctx.setOperation(IndexOperation.UPDATE);
        return lsmHarness.modify(ctx, true, tuple);
    }

    @Override
    public boolean tryUpsert(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.UPSERT);
        return lsmHarness.modify(ctx, true, tuple);
    }

    @Override
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
        ctx.setOperation(IndexOperation.SEARCH);
        lsmHarness.search(ctx, cursor, searchPred);
    }

    @Override
    public void flush(ILSMIOOperation operation) throws HyracksDataException {
        lsmHarness.flush(operation);
    }

    @Override
    public void merge(ILSMIOOperation operation) throws HyracksDataException {
        lsmHarness.merge(operation);
    }

    @Override
    public void physicalDelete(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.PHYSICALDELETE);
        lsmHarness.modify(ctx, false, tuple);
    }

    @Override
    public ILSMIOOperation scheduleFlush() throws HyracksDataException {
        ctx.setOperation(IndexOperation.FLUSH);
        return lsmHarness.scheduleFlush(ctx);
    }

    @Override
    public ILSMIOOperation scheduleMerge(List<ILSMDiskComponent> components) throws HyracksDataException {
        ctx.setOperation(IndexOperation.MERGE);
        ctx.getComponentsToBeMerged().clear();
        ctx.getComponentsToBeMerged().addAll(components);
        return lsmHarness.scheduleMerge(ctx);
    }

    @Override
    public void scheduleReplication(List<ILSMDiskComponent> lsmComponents, LSMOperationType opType)
            throws HyracksDataException {
        ctx.setOperation(IndexOperation.REPLICATE);
        ctx.getComponentsToBeReplicated().clear();
        ctx.getComponentsToBeReplicated().addAll(lsmComponents);
        lsmHarness.scheduleReplication(ctx, lsmComponents, opType);
    }

    @Override
    public ILSMIOOperation scheduleFullMerge() throws HyracksDataException {
        ctx.setOperation(IndexOperation.FULL_MERGE);
        return lsmHarness.scheduleFullMerge(ctx);
    }

    @Override
    public void forcePhysicalDelete(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.PHYSICALDELETE);
        lsmHarness.forceModify(ctx, tuple);
    }

    @Override
    public void forceInsert(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.INSERT);
        lsmHarness.forceModify(ctx, tuple);
    }

    @Override
    public void forceUpsert(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.UPSERT);
        lsmHarness.forceModify(ctx, tuple);
    }

    @Override
    public void forceDelete(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.DELETE);
        lsmHarness.forceModify(ctx, tuple);
    }

    @Override
    public void updateMeta(IValueReference key, IValueReference value) throws HyracksDataException {
        ctx.setOperation(IndexOperation.UPSERT);
        lsmHarness.updateMeta(ctx, key, value);
    }

    @Override
    public void forceUpdateMeta(IValueReference key, IValueReference value) throws HyracksDataException {
        ctx.setOperation(IndexOperation.UPSERT);
        lsmHarness.forceUpdateMeta(ctx, key, value);
    }

    @Override
    public IIndexCursor createSearchCursor(boolean exclusive) {
        return cursorFactory.create(ctx);
    }

    @Override
    public void updateFilter(ITupleReference tuple) throws HyracksDataException {
        ctx.setOperation(IndexOperation.UPSERT);
        lsmHarness.updateFilter(ctx, tuple);
    }

    public void batchOperate(FrameTupleAccessor accessor, FrameTupleReference tuple, IFrameTupleProcessor processor,
            IFrameOperationCallback frameOpCallback) throws HyracksDataException {
        lsmHarness.batchOperate(ctx, accessor, tuple, processor, frameOpCallback);
    }

    @Override
    public void scanDiskComponents(IIndexCursor cursor) throws HyracksDataException {
        ctx.setOperation(IndexOperation.DISK_COMPONENT_SCAN);
        lsmHarness.scanDiskComponents(ctx, cursor);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ':' + lsmHarness.toString();
    }

    @Override
    public void deleteComponents(Predicate<ILSMComponent> predicate) throws HyracksDataException {
        lsmHarness.deleteComponents(ctx, predicate);
    }

    @Override
    public ILSMIndexOperationContext getOpContext() {
        return ctx;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        ctx.destroy();
    }
}
