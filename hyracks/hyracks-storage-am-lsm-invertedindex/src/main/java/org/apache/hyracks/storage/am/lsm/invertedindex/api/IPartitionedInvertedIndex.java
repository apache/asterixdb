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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.InvertedListPartitions;

public interface IPartitionedInvertedIndex {
    public boolean openInvertedListPartitionCursors(IInvertedIndexSearcher searcher, IIndexOperationContext ictx,
            short numTokensLowerBound, short numTokensUpperBound, InvertedListPartitions invListPartitions,
            ArrayList<IInvertedListCursor> cursorsOrderedByTokens) throws HyracksDataException, IndexException;

    public boolean isEmpty();
}
