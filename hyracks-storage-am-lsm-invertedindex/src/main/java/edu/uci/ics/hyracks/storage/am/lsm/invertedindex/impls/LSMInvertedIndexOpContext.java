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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;

public class LSMInvertedIndexOpContext implements IIndexOpContext {
    
    private IndexOp op;
    //private final MultiComparator cmp;
    //private final int invListFieldCount;
    //private final int tokenFieldCount;
    private final IInvertedIndex memInvIndex;
    private final IIndex memDeletedKeysBTree;
    
    // Accessor to the in-memory inverted index.
    public IInvertedIndexAccessor insertAccessor;
    // Accessor to the deleted-keys BTree.
    public IIndexAccessor deleteAccessor;
    
    public LSMInvertedIndexOpContext(IInvertedIndex memInvIndex, IIndex memDeletedKeysBTree) {
        this.memInvIndex = memInvIndex;
        this.memDeletedKeysBTree = memDeletedKeysBTree;
        /*
    	this.cmp = MultiComparator.create(btree.getComparatorFactories());
    	this.invListFieldCount = memoryBTreeInvertedIndex.getInvListCmpFactories().length;
    	this.tokenFieldCount = cmp.getKeyFieldCount() - invListFieldCount;
    	*/
    }
    
    @Override
    public void reset() {
    }

    @Override
    // TODO: Ignore opcallback for now.
    public void reset(IndexOp newOp) {
        switch (newOp) {
            case INSERT: {
                if (insertAccessor == null) {
                    insertAccessor = (IInvertedIndexAccessor) memInvIndex.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                }
                break;
            }
            case DELETE: {
                if (deleteAccessor == null) {
                    deleteAccessor = memDeletedKeysBTree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                }
                break;
            }
        }
        op = newOp;
    }
    
    /*
    public int getInvListFieldCount() {
    	return invListFieldCount;
    }
    
    public int getTokenFieldCount() {
    	return tokenFieldCount;
    }
    
    public MultiComparator getComparator() {
    	return cmp;
    }
    */
}
