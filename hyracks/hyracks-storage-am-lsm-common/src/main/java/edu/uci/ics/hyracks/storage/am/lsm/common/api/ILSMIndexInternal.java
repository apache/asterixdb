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

package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public interface ILSMIndexInternal extends ILSMIndex {
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

    public void addComponent(ILSMComponent index);

    public void subsumeMergedComponents(ILSMComponent newComponent, List<ILSMComponent> mergedComponents);

    public void changeMutableComponent();

    public void changeFlushStatusForCurrentMutableCompoent(boolean needsFlush);

    public boolean hasFlushRequestForCurrentMutableComponent();

    /**
     * Populates the context's component holder with a snapshot of the components involved in the operation.
     * 
     * @param ctx
     *            - the operation's context
     */
    public void getOperationalComponents(ILSMIndexOperationContext ctx);

    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException;

}
