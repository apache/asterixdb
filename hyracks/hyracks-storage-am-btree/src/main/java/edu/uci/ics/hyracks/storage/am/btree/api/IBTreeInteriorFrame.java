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

package edu.uci.ics.hyracks.storage.am.btree.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public interface IBTreeInteriorFrame extends ITreeIndexFrame {
    public void insertSorted(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException;

    public int getChildPageId(RangePredicate pred, MultiComparator srcCmp);

    public int getLeftmostChildPageId(MultiComparator cmp);

    public int getRightmostChildPageId(MultiComparator cmp);

    public void setRightmostChildPageId(int pageId);

    public void deleteGreatest(MultiComparator cmp);
}
