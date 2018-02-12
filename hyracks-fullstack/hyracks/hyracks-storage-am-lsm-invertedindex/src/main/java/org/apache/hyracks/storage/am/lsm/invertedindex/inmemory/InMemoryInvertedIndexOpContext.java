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

package org.apache.hyracks.storage.am.lsm.invertedindex.inmemory;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTokenizingTupleIterator;
import org.apache.hyracks.storage.common.MultiComparator;

public class InMemoryInvertedIndexOpContext implements IIndexOperationContext {
    protected final BTree btree;
    protected final IBinaryComparatorFactory[] tokenCmpFactories;
    private IndexOperation op;

    // Needed for search operations,
    private RangePredicate btreePred;
    private BTreeAccessor btreeAccessor;
    private MultiComparator btreeCmp;

    private MultiComparator tokenFieldsCmp;

    // To generate in-memory BTree tuples for insertions.
    private final IBinaryTokenizerFactory tokenizerFactory;
    private InvertedIndexTokenizingTupleIterator tupleIter;
    private boolean destroyed = false;

    InMemoryInvertedIndexOpContext(BTree btree, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory) {
        this.btree = btree;
        this.tokenCmpFactories = tokenCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        switch (newOp) {
            case INSERT:
            case DELETE: {
                if (getTupleIter() == null) {
                    setTokenizingTupleIterator();
                }
                break;
            }
            case SEARCH: {
                if (getBtreePred() == null) {
                    btreePred = new RangePredicate(null, null, true, true, null, null);
                    btreeAccessor = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
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

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    protected void setTokenizingTupleIterator() {
        IBinaryTokenizer tokenizer = getTokenizerFactory().createTokenizer();
        tupleIter = new InvertedIndexTokenizingTupleIterator(tokenCmpFactories.length,
                btree.getFieldCount() - tokenCmpFactories.length, tokenizer);
    }

    public InvertedIndexTokenizingTupleIterator getTupleIter() {
        return tupleIter;
    }

    public BTreeAccessor getBtreeAccessor() {
        return btreeAccessor;
    }

    public RangePredicate getBtreePred() {
        return btreePred;
    }

    public MultiComparator getTokenFieldsCmp() {
        return tokenFieldsCmp;
    }

    public MultiComparator getBtreeCmp() {
        return btreeCmp;
    }

    public IBinaryTokenizerFactory getTokenizerFactory() {
        return tokenizerFactory;
    }

    public void setTupleIter(InvertedIndexTokenizingTupleIterator tupleIter) {
        this.tupleIter = tupleIter;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        if (btreeAccessor != null) {
            btreeAccessor.destroy();
        }
    }
}
