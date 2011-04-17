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

public class FindPathList {
    private int[] pageIds;
    private int[] pageLsns;
    private int[] parentIndexes;
    private int size;
    private final int growth;
    private int first;

    public FindPathList(int initialCapacity, int growth) {
        pageIds = new int[initialCapacity];
        pageLsns = new int[initialCapacity];
        parentIndexes = new int[initialCapacity];
        size = 0;
        this.growth = growth;
        first = 0;
    }

    public int size() {
        return size;
    }

    public void add(int pageId, int parentId) {
        if (size == pageIds.length) {
            int[] newPageIds = new int[pageIds.length + growth];
            System.arraycopy(pageIds, 0, newPageIds, 0, pageIds.length);
            pageIds = newPageIds;

            int[] newPageLsns = new int[pageLsns.length + growth];
            System.arraycopy(pageLsns, 0, newPageLsns, 0, pageLsns.length);
            pageLsns = newPageLsns;
            
            int[] newParentIds = new int[parentIndexes.length + growth];
            System.arraycopy(parentIndexes, 0, newParentIds, 0, parentIndexes.length);
            parentIndexes = newParentIds;
        }

        pageIds[size] = pageId;
        parentIndexes[size] = parentId;
        size++;
    }

    public void moveFirst() {
        first++;
    }

    public int getFirstPageId() {
        return pageIds[first];
    }
    
    public int getFirstPageLsn() {
        return pageLsns[first];
    }

    public int getFirstParentIndex() {
        return parentIndexes[first];
    }

    public int getPageId(int i) {
        return pageIds[i];
    }
    
    public int getPageLsn(int i) {
        return pageLsns[i];
    }
    
    public void setPageLsn(int i, int pageLsn) {
        pageLsns[i] = pageLsn;
    }

    public int getParentIndex(int i) {
        return parentIndexes[i];
    }
    
    public boolean isLast() {
        return size == first;
    }

    public void clear() {
        size = 0;
        first = 0;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}
