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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;

public class InvertedIndexSearchPredicate implements ISearchPredicate {
    private static final long serialVersionUID = 1L;

    private ITupleReference queryTuple;
    private int queryFieldIndex;
    private final IBinaryTokenizer queryTokenizer;
    private final IInvertedIndexSearchModifier searchModifier;    
    
    public InvertedIndexSearchPredicate(IBinaryTokenizer queryTokenizer, IInvertedIndexSearchModifier searchModifier) {
        this.queryTokenizer = queryTokenizer;
        this.searchModifier = searchModifier;
    }
    
    public void setQueryTuple(ITupleReference queryTuple) {
        this.queryTuple = queryTuple;
    }
    
    public ITupleReference getQueryTuple() {
        return queryTuple;
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
}
