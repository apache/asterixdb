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

import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;

public class InMemoryBtreeInvertedIndexOpContext implements IIndexOpContext {
    public IndexOp op;
    public final int numBTreeFields;
    
    // To generate in-memory btree tuples for insertions.
    public ArrayTupleBuilder btreeTupleBuilder;
    public ArrayTupleReference btreeTupleReference;
    
    public InMemoryBtreeInvertedIndexOpContext(int numBTreeFields) {
        this.numBTreeFields = numBTreeFields;
    }
    
    @Override
    public void reset() {
        op = null;
    }

    @Override
    public void reset(IndexOp newOp) {
        switch (newOp) {
            case INSERT: {
                btreeTupleBuilder = new ArrayTupleBuilder(numBTreeFields);
                btreeTupleReference = new ArrayTupleReference();                
                break;
            }
            case SEARCH: {
                break;
            }
            default: {
                throw new UnsupportedOperationException("Unsupported operation " + newOp);
            }
        }
        op = newOp;
    }
}
