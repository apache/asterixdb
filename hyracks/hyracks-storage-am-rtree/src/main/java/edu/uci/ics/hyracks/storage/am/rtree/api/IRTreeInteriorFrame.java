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

package edu.uci.ics.hyracks.storage.am.rtree.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.PathList;

public interface IRTreeInteriorFrame extends IRTreeFrame {

    public int findBestChild(ITupleReference tuple, MultiComparator cmp);

    public boolean checkIfEnlarementIsNeeded(ITupleReference tuple, MultiComparator cmp);

    public int getChildPageId(int tupleIndex);

    public int getChildPageIdIfIntersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp);

    public int findTupleByPointer(ITupleReference tuple, MultiComparator cmp);

    public int findTupleByPointer(ITupleReference tuple, PathList traverseList, int parentIndex, MultiComparator cmp);

    public void adjustKey(ITupleReference tuple, int tupleIndex, MultiComparator cmp) throws TreeIndexException;

    public void enlarge(ITupleReference tuple, MultiComparator cmp);

}
