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

public class TraverseList {
    private IntArrayList pageIds;
    private IntArrayList pageLsns;
    private IntArrayList parentIndexes;
    
    public TraverseList(int initialCapacity, int growth) {
        pageIds = new IntArrayList(initialCapacity, growth);
        pageLsns = new IntArrayList(initialCapacity, growth);
        parentIndexes = new IntArrayList(initialCapacity, growth);
    }

    public int size() {
        return pageIds.size();
    }
    
    public int first() {
        return pageIds.first();
    }

    public void add(int pageId, int pageLsn, int parentIndex) {
        pageIds.add(pageId);
        pageLsns.add(pageLsn);
        parentIndexes.add(parentIndex);
    }

    public int getFirstPageId() {
        return pageIds.getFirst();
    }
    
    public int getFirstPageLsn() {
        return pageLsns.getFirst();
    }

    public int getFirstParentIndex() {
        return parentIndexes.getFirst();
    }
    
    public int getLastPageId() {
        return pageIds.getLast();
    }
    
    public int getLastPageLsn() {
        return pageLsns.getLast();
    }

    public int getLastParentIndex() {
        return parentIndexes.getLast();
    }

    public int getPageId(int i) {
        return pageIds.get(i);
    }
    
    public int getPageLsn(int i) {
        return pageLsns.get(i);
    }

    public int getParentIndex(int i) {
        return parentIndexes.get(i);
    }
    
    public void setPageLsn(int i, int pageLsn) {
        pageLsns.set(i, pageLsn);
    }
    
    public void moveFirst() {
        pageIds.moveFirst();
        pageLsns.moveFirst();
        parentIndexes.moveFirst();
    }
    
    public void moveLast() {
        pageIds.removeLast();
        pageLsns.removeLast();
        parentIndexes.removeLast();
    }
    
    public boolean isLast() {
        return pageIds.isLast();
    }

    public void clear() {
        pageIds.clear();
        pageLsns.clear();
        parentIndexes.clear();
    }

    public boolean isEmpty() {
        return pageIds.isEmpty();
    }
}
