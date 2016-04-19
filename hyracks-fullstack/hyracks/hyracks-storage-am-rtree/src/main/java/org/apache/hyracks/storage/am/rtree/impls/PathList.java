/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.rtree.impls;

import org.apache.hyracks.storage.common.arraylist.IntArrayList;
import org.apache.hyracks.storage.common.arraylist.LongArrayList;

public class PathList {
    private IntArrayList pageIds;
    private LongArrayList pageLsns;
    private IntArrayList pageIndexes;

    public PathList(int initialCapacity, int growth) {
        pageIds = new IntArrayList(initialCapacity, growth);
        pageLsns = new LongArrayList(initialCapacity, growth);
        pageIndexes = new IntArrayList(initialCapacity, growth);
    }

    public int size() {
        return pageIds.size();
    }

    public int first() {
        return pageIds.first();
    }

    public void add(int pageId, long pageLsn, int pageIndex) {
        pageIds.add(pageId);
        pageLsns.add(pageLsn);
        pageIndexes.add(pageIndex);
    }

    public void addFirst(int pageId, long pageLsn, int pageIndex) {
        pageIds.addFirst(pageId);
        pageLsns.addFirst(pageLsn);
        pageIndexes.addFirst(pageIndex);
    }

    public int getFirstPageId() {
        return pageIds.getFirst();
    }

    public long getFirstPageLsn() {
        return pageLsns.getFirst();
    }

    public int getFirstPageIndex() {
        return pageIndexes.getFirst();
    }

    public int getLastPageId() {
        return pageIds.getLast();
    }

    public long getLastPageLsn() {
        return pageLsns.getLast();
    }

    public int getLastPageIndex() {
        return pageIndexes.getLast();
    }

    public int getPageId(int i) {
        return pageIds.get(i);
    }

    public long getPageLsn(int i) {
        return pageLsns.get(i);
    }

    public int getPageIndex(int i) {
        return pageIndexes.get(i);
    }

    public void setPageLsn(int i, long pageLsn) {
        pageLsns.set(i, pageLsn);
    }

    public void moveFirst() {
        pageIds.moveFirst();
        pageLsns.moveFirst();
        pageIndexes.moveFirst();
    }

    public void moveLast() {
        pageIds.removeLast();
        pageLsns.removeLast();
        pageIndexes.removeLast();
    }

    public boolean isLast() {
        return pageIds.isLast();
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
