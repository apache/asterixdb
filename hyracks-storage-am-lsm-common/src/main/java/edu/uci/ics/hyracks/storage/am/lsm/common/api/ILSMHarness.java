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

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public interface ILSMHarness {
    public boolean insertUpdateOrDelete(ITupleReference tuple, ILSMIndexOperationContext ictx, boolean tryOperation)
            throws HyracksDataException, IndexException;

    public boolean noOp(ILSMIndexOperationContext ictx, boolean tryOperation) throws HyracksDataException;

    public List<ILSMComponent> search(IIndexCursor cursor, ISearchPredicate pred, ILSMIndexOperationContext ctx,
            boolean includeMutableComponent) throws HyracksDataException, IndexException;

    // Eventually includeMutableComponent and ctx should be removed.
    public void closeSearchCursor(List<ILSMComponent> operationalComponents, boolean includeMutableComponent,
            ILSMIndexOperationContext ctx) throws HyracksDataException;

    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException,
            IndexException;

    public void merge(ILSMIOOperation operation) throws HyracksDataException, IndexException;

    public void flush(ILSMIOOperation operation) throws HyracksDataException, IndexException;

    public void addBulkLoadedComponent(ILSMComponent index) throws HyracksDataException, IndexException;

    public ILSMIndex getIndex();

    public ILSMFlushController getFlushController();

    public ILSMOperationTracker getOperationTracker();

    public ILSMIOOperationScheduler getIOScheduler();
}
