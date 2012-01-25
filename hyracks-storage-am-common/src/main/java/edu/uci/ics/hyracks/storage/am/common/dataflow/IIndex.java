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

package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Interface describing the operations common to all indexes.
 */
public interface IIndex {
    /**
     * Initializes the persistent state of an index, e.g., the root page,
     * and metadata pages.
     * 
     * @param indexFileId
     *            The file id to use for this index.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     */
    public void create(int indexFileId) throws HyracksDataException;

    /**
     * Opens the index backed by the given file id.
     * 
     * @param indexFileId
     *            The file id backing this index.
     */
    public void open(int indexFileId) throws HyracksDataException;
    
    /**
     * Closes the index.
     */
    public void close();
}
