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

package edu.uci.ics.hyracks.storage.am.rtree.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.Rectangle;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TupleEntryArrayList;

public interface IRTreeFrame extends ITreeIndexFrame {

	public ITreeIndexTupleReference createTupleReference();

	public void computeMBR(ISplitKey splitKey, MultiComparator cmp);

	public void insert(ITupleReference tuple, MultiComparator cmp,
			int tupleIndex) throws Exception;

	public void delete(int tupleIndex, MultiComparator cmp) throws Exception;

	public int getPageNsn();

	public void setPageNsn(int pageNsn);

	public int getRightPage();

	public void setRightPage(int rightPage);

	public void adjustMBR(ITreeIndexTupleReference[] tuples, MultiComparator cmp);

}