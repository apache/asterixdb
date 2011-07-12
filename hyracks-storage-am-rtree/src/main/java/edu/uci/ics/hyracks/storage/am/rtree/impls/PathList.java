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

package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.storage.am.common.ophelpers.IntArrayList;

public class PathList {
    private IntArrayList pageIds;
    private IntArrayList pageLsns;
    private IntArrayList pageIndexes;
    
    public PathList(int initialCapacity, int growth) {
        pageIds = new IntArrayList(initialCapacity, growth);
        pageLsns = new IntArrayList(initialCapacity, growth);
        pageIndexes = new IntArrayList(initialCapacity, growth);
    }

    public int size() {
        return pageIds.size();
    }

    public void add(int pageId, int pageLsn, int pageIndex) {
        pageIds.add(pageId);
        pageLsns.add(pageLsn);
        pageIndexes.add(pageIndex);
    }

    public int getLastPageId() {
        return pageIds.getLast();
    }
    
    public int getLastPageLsn() {
        return pageLsns.getLast();
    }
    
    public int getLastPageIndex() {
        return pageIndexes.getLast();
    }

    public int getPageId(int i) {
        return pageIds.get(i);
    }
    
    public int getPageLsn(int i) {
        return pageLsns.get(i);
    }
    
    public int getPageIndex(int i) {
        return pageIndexes.get(i);
    }
    
    public void removeLast() {
        pageIds.removeLast();
        pageLsns.removeLast();
        pageIndexes.removeLast();
    }
    
    public void clear() {
        pageIds.clear();
        pageLsns.clear();
        pageIndexes.clear();
    }

    public boolean isEmpty() {
        return pageIds.isEmpty();
    }
}
