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

package edu.uci.ics.hyracks.storage.am.btree.interfaces;

import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;

public interface IBTreeFrameInterior extends IBTreeFrame {
	//public int getChildPageId(IFieldAccessor[] fields, MultiComparator cmp);
	public int getChildPageId(RangePredicate pred, MultiComparator srcCmp);
	public int getLeftmostChildPageId(MultiComparator cmp);
	public int getRightmostChildPageId(MultiComparator cmp);
	public void setRightmostChildPageId(int pageId);
	public void deleteGreatest(MultiComparator cmp);
}
