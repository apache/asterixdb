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
package org.apache.asterix.test.ioopcallbacks;

import java.util.List;
import java.util.function.Predicate;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class TestLSMIndexAccessor implements ILSMIndexAccessor {

    private final ILSMIndexOperationContext opCtx;

    public TestLSMIndexAccessor(ILSMIndexOperationContext opCtx) {
        this.opCtx = opCtx;
    }

    @Override
    public void insert(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void upsert(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IIndexCursor createSearchCursor(boolean exclusive) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy() throws HyracksDataException {
    }

    @Override
    public ILSMIndexOperationContext getOpContext() {
        return opCtx;
    }

    @Override
    public ILSMIOOperation scheduleFlush() throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILSMIOOperation scheduleMerge(List<ILSMDiskComponent> components) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILSMIOOperation scheduleFullMerge() throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void physicalDelete(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryInsert(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryDelete(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryUpdate(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryUpsert(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forcePhysicalDelete(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceInsert(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceDelete(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceUpsert(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void scheduleReplication(List<ILSMDiskComponent> diskComponents, LSMOperationType opType)
            throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush(ILSMIOOperation operation) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void merge(ILSMIOOperation operation) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateMeta(IValueReference key, IValueReference value) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceUpdateMeta(IValueReference key, IValueReference value) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void scanDiskComponents(IIndexCursor cursor) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteComponents(Predicate<ILSMComponent> predicate) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateFilter(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }
}
