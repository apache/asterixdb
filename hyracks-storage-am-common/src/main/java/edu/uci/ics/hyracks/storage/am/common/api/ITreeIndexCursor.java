/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public interface ITreeIndexCursor {
    public void reset();

    public boolean hasNext() throws Exception;

    public void next() throws Exception;

    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws Exception;

    public ICachedPage getPage();

    public void close() throws Exception;

    public void setBufferCache(IBufferCache bufferCache);

    public void setFileId(int fileId);

    public ITupleReference getTuple();
}
