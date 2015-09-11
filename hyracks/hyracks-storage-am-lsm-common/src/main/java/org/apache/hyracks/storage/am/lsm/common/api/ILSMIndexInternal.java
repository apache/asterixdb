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

package org.apache.hyracks.storage.am.lsm.common.api;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;

public interface ILSMIndexInternal extends ILSMIndex {
    @Override
    public ILSMIndexAccessorInternal createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) throws HyracksDataException;

    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException, IndexException;

    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException;

    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException;

    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException;

    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException;

    public ILSMComponent merge(ILSMIOOperation operation) throws HyracksDataException, IndexException;

    public void addComponent(ILSMComponent index) throws HyracksDataException;

    public void subsumeMergedComponents(ILSMComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException;

    public void changeMutableComponent();

    public void changeFlushStatusForCurrentMutableCompoent(boolean needsFlush);

    public boolean hasFlushRequestForCurrentMutableComponent();

    /**
     * Populates the context's component holder with a snapshot of the components involved in the operation.
     *
     * @param ctx
     *            - the operation's context
     * @throws HyracksDataException
     */
    public void getOperationalComponents(ILSMIndexOperationContext ctx) throws HyracksDataException;

    public List<ILSMComponent> getInactiveDiskComponents();

    public void addInactiveDiskComponent(ILSMComponent diskComponent);

    /**
     * Persistent the LSM component
     *
     * @param lsmComponent
     *            , the component to be persistent
     * @throws HyracksDataException
     */
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException;

    public boolean isCurrentMutableComponentEmpty() throws HyracksDataException;
    
    public void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMComponent> lsmComponents, boolean bulkload,
            ReplicationOperation operation) throws HyracksDataException;

}
