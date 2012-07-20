/*
 * Copyright 2009-2012 by The Regents of the University of California
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
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public interface ILSMHarness {
    public void insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException,
            IndexException;

    public List<Object> search(IIndexCursor cursor, ISearchPredicate pred, IIndexOpContext ctx,
            boolean includeMemComponent) throws HyracksDataException, IndexException;

    public void closeSearchCursor(AtomicInteger searcherRefCount, boolean includeMemComponent)
            throws HyracksDataException;

    public void merge(ILSMIOOperation operation) throws HyracksDataException, IndexException;

    public void flush(ILSMIOOperation operation) throws HyracksDataException, IndexException;

    public void addBulkLoadedComponent(Object index) throws HyracksDataException;
    
    public ILSMIndex getIndex();

    public ILSMFlushController getFlushController();

    public ILSMOperationTracker getOperationTracker();

    public ILSMIOOperationScheduler getIOScheduler();
}
