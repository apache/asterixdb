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
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigEvaluator;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.IFullTextConfigEvaluatorFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.PartitionedInvertedIndexTokenizingTupleIterator;

public class PartitionedInMemoryInvertedIndexOpContext extends InMemoryInvertedIndexOpContext {

    public PartitionedInMemoryInvertedIndexOpContext(BTree btree, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, IFullTextConfigEvaluatorFactory fullTextConfigEvaluatorFactory) {
        super(btree, tokenCmpFactories, tokenizerFactory, fullTextConfigEvaluatorFactory);
    }

    protected void setTokenizingTupleIterator() {
        IBinaryTokenizer tokenizer = getTokenizerFactory().createTokenizer();
        IFullTextConfigEvaluator fullTextConfigEvaluator =
                getFullTextConfigEvaluatorFactory().createFullTextConfigEvaluator();
        setTupleIter(new PartitionedInvertedIndexTokenizingTupleIterator(tokenCmpFactories.length,
                btree.getFieldCount() - tokenCmpFactories.length, tokenizer, fullTextConfigEvaluator));
    }
}
