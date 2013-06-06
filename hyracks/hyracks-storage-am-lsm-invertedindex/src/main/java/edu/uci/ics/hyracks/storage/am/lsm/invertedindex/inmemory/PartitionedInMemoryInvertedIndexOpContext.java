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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.PartitionedInvertedIndexTokenizingTupleIterator;

public class PartitionedInMemoryInvertedIndexOpContext extends InMemoryInvertedIndexOpContext {

    public PartitionedInMemoryInvertedIndexOpContext(BTree btree, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory) {
        super(btree, tokenCmpFactories, tokenizerFactory);
    }

    protected void setTokenizingTupleIterator() {
        IBinaryTokenizer tokenizer = tokenizerFactory.createTokenizer();
        tupleIter = new PartitionedInvertedIndexTokenizingTupleIterator(tokenCmpFactories.length, btree.getFieldCount()
                - tokenCmpFactories.length, tokenizer);
    }
}
