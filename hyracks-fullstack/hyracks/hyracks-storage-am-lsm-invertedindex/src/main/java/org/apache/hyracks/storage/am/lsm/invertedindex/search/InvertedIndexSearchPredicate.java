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

package org.apache.hyracks.storage.am.lsm.invertedindex.search;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.impls.AbstractSearchPredicate;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.common.MultiComparator;

public class InvertedIndexSearchPredicate extends AbstractSearchPredicate {
    private static final long serialVersionUID = 1L;

    private ITupleReference queryTuple;
    private int queryFieldIndex;
    private final IBinaryTokenizer queryTokenizer;
    private final IInvertedIndexSearchModifier searchModifier;
    // Keeps the information whether the given query is a full-text search or not.
    // We need to have this information to stop the search process since we don't allow a phrase search yet.
    private boolean isFullTextSearchQuery;

    public InvertedIndexSearchPredicate(IBinaryTokenizer queryTokenizer, IInvertedIndexSearchModifier searchModifier) {
        this.queryTokenizer = queryTokenizer;
        this.searchModifier = searchModifier;
        this.isFullTextSearchQuery = false;
    }

    public InvertedIndexSearchPredicate(IBinaryTokenizer queryTokenizer, IInvertedIndexSearchModifier searchModifier,
            ITupleReference minFilterTuple, ITupleReference maxFilterTuple, boolean isFullTextSearchQuery) {
        super(minFilterTuple, maxFilterTuple);
        this.queryTokenizer = queryTokenizer;
        this.searchModifier = searchModifier;
        this.isFullTextSearchQuery = isFullTextSearchQuery;
    }

    public void setQueryTuple(ITupleReference queryTuple) {
        this.queryTuple = queryTuple;
    }

    public ITupleReference getQueryTuple() {
        return queryTuple;
    }

    public void setIsFullTextSearchQuery(boolean isFullTextSearchQuery) {
        this.isFullTextSearchQuery = isFullTextSearchQuery;
    }

    public boolean getIsFullTextSearchQuery() {
        return isFullTextSearchQuery;
    }

    public void setQueryFieldIndex(int queryFieldIndex) {
        this.queryFieldIndex = queryFieldIndex;
    }

    public int getQueryFieldIndex() {
        return queryFieldIndex;
    }

    public IInvertedIndexSearchModifier getSearchModifier() {
        return searchModifier;
    }

    public IBinaryTokenizer getQueryTokenizer() {
        return queryTokenizer;
    }

    @Override
    public MultiComparator getLowKeyComparator() {
        // TODO: This doesn't make sense for an inverted index. Change ISearchPredicate interface.
        return null;
    }

    @Override
    public MultiComparator getHighKeyComparator() {
        // TODO: This doesn't make sense for an inverted index. Change ISearchPredicate interface.
        return null;
    }

    @Override
    public ITupleReference getLowKey() {
        return null;
    }
}
