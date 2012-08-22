/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexFileManager.LSMInvertedIndexFileNameComponent;

public class LSMInvertedIndexAccessor implements ILSMIndexAccessor, IInvertedIndexAccessor {

    protected final LSMHarness lsmHarness;    
    protected final ILSMIndexFileManager fileManager;
    protected final IIndexOpContext ctx;
    protected final LSMInvertedIndex invIndex;
    
    public LSMInvertedIndexAccessor(LSMInvertedIndex invIndex, LSMHarness lsmHarness, ILSMIndexFileManager fileManager, IIndexOpContext ctx) {
        this.lsmHarness = lsmHarness;
        this.fileManager = fileManager;
        this.ctx = ctx;
        this.invIndex = invIndex;
    }

    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
        ctx.reset(IndexOp.INSERT);
        lsmHarness.insertUpdateOrDelete(tuple, ctx);
    }

    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        ctx.reset(IndexOp.SEARCH);
        lsmHarness.search(cursor, searchPred, ctx, true);
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub
        
    }
    
    public IIndexCursor createSearchCursor() {
        return new LSMInvertedIndexSearchCursor(); 
    }
    
    @Override
    public ILSMIOOperation createFlushOperation(ILSMIOOperationCallback callback) {
        LSMInvertedIndexFileNameComponent fileNameComponent = (LSMInvertedIndexFileNameComponent) fileManager
                .getRelFlushFileName();
        FileReference dictBTreeFileRef = fileManager.createFlushFile(fileNameComponent.getDictBTreeFileName());
        FileReference deletedKeysBTreeFileRef = fileManager.createFlushFile(fileNameComponent
                .getDeletedKeysBTreeFileName());
        return new LSMInvertedIndexFlushOperation(lsmHarness.getIndex(), dictBTreeFileRef, deletedKeysBTreeFileRef,
                callback);
    }
    
    @Override
    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException,
            IndexException {
        ILSMIOOperation mergeOp = invIndex.createMergeOperation(callback);
        return mergeOp;
    }

    @Override
    public void flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        lsmHarness.flush(operation);
    }

    @Override
    public void merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        lsmHarness.merge(operation);
    }

    @Override
    public IIndexCursor createRangeSearchCursor() {
        return new LSMInvertedIndexRangeSearchCursor();
    }
    
    @Override
    public void rangeSearch(IIndexCursor cursor, ISearchPredicate searchPred) throws IndexException,
            HyracksDataException {
        search(cursor, searchPred);
    }
    
    @Override
    public void physicalDelete(ITupleReference tuple) throws HyracksDataException, IndexException {
        // TODO: Do we need this?
    }

    @Override
    public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void upsert(ITupleReference tuple) throws HyracksDataException, IndexException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public IInvertedListCursor createInvertedListCursor() {
        throw new UnsupportedOperationException("Cannot create inverted list cursor on lsm inverted index.");
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference searchKey)
            throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Cannot open inverted list cursor on lsm inverted index.");}
}
