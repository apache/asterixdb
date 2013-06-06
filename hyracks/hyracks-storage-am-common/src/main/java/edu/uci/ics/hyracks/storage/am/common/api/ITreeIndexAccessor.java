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

package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Client handle for performing operations
 * (insert/delete/update/search/diskorderscan) on an ITreeIndex. An
 * ITreeIndexAccessor is not thread safe, but different ITreeIndexAccessors can
 * concurrently operate on the same ITreeIndex (i.e., the ITreeIndex must allow
 * concurrent operations).
 */
public interface ITreeIndexAccessor extends IIndexAccessor {
	/**
	 * Creates a cursor appropriate for passing into diskOrderScan().
	 * 
	 */
	public ITreeIndexCursor createDiskOrderScanCursor();
	
	/**
	 * Open the given cursor for a disk-order scan, positioning the cursor to
	 * the first leaf tuple.
	 * 
	 * @param icursor
	 *            Cursor to be opened for disk-order scanning.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 */
	public void diskOrderScan(ITreeIndexCursor cursor)
			throws HyracksDataException;
}
