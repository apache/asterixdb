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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexInsertTupleIterator;

public class InMemoryInvertedIndexOpContext implements IIndexOpContext {
    public IndexOp op;
    public final BTree btree;

    // Needed for search operations,    
    public RangePredicate btreePred;
    public BTreeAccessor btreeAccessor;
    public MultiComparator btreeCmp;
    public IBinaryComparatorFactory[] tokenCmpFactories;
    public MultiComparator tokenFieldsCmp;

    // To generate in-memory BTree tuples for insertions.
    private final IBinaryTokenizerFactory tokenizerFactory;
    public InvertedIndexInsertTupleIterator insertTupleIter;

    public InMemoryInvertedIndexOpContext(BTree btree, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory) {
        this.btree = btree;
        this.tokenCmpFactories = tokenCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
    }

    @Override
    public void reset(IndexOp newOp) {
        switch (newOp) {
            case INSERT: {
                IBinaryTokenizer tokenizer = tokenizerFactory.createTokenizer();
                insertTupleIter = new InvertedIndexInsertTupleIterator(tokenCmpFactories.length, btree.getFieldCount()
                        - tokenCmpFactories.length, tokenizer);
                break;
            }
            case SEARCH: {
                if (btreePred == null) {
                    btreePred = new RangePredicate(null, null, true, true, null, null);
                    // TODO: Ignore opcallbacks for now.
                    btreeAccessor = (BTreeAccessor) btree.createAccessor(NoOpOperationCallback.INSTANCE,
                            NoOpOperationCallback.INSTANCE);
                    btreeCmp = MultiComparator.create(btree.getComparatorFactories());
                    tokenFieldsCmp = MultiComparator.create(tokenCmpFactories);
                }
                break;
            }
            default: {
                throw new UnsupportedOperationException("Unsupported operation " + newOp);
            }
        }
        op = newOp;
    }

    @Override
    public void reset() {
        op = null;
    }
}
