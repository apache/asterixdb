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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;

/**
 * A search cursor that fetches the result from an IInvertedIndexSearcher instance.
 */
public class OnDiskInvertedIndexSearchCursor extends EnforcedIndexCursor {

    private final IInvertedIndexSearcher invIndexSearcher;

    public OnDiskInvertedIndexSearchCursor(IInvertedIndexSearcher invIndexSearcher) {
        this.invIndexSearcher = invIndexSearcher;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // No-op for this cursor since all necessary information is already set in the given searcher.
        // This class is just a wrapper.
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        return invIndexSearcher.hasNext();
    }

    @Override
    public void doNext() throws HyracksDataException {
        invIndexSearcher.next();
    }

    @Override
    public ITupleReference doGetTuple() {
        return invIndexSearcher.getTuple();
    }

    @Override
    public void doClose() throws HyracksDataException {
        doDestroy();
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        if (invIndexSearcher != null) {
            invIndexSearcher.destroy();
        }
    }

}
