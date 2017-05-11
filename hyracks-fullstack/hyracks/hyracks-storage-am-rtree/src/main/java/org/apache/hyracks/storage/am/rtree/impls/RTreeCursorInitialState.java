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

import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class RTreeCursorInitialState implements ICursorInitialState {

    private PathList pathList;
    private int rootPage;
    private ICachedPage page; // for disk order scan
    private MultiComparator originalKeyCmp;

    public RTreeCursorInitialState(PathList pathList, int rootPage) {
        this.pathList = pathList;
        this.rootPage = rootPage;
    }

    public PathList getPathList() {
        return pathList;
    }

    public int getRootPage() {
        return rootPage;
    }

    public void setRootPage(int rootPage) {
        this.rootPage = rootPage;
    }

    public ICachedPage getPage() {
        return page;
    }

    public void setPage(ICachedPage page) {
        this.page = page;
    }

    @Override
    public MultiComparator getOriginalKeyComparator() {
        return originalKeyCmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        this.originalKeyCmp = originalCmp;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return null;
    }

    @Override
    public void setSearchOperationCallback(ISearchOperationCallback searchCallback) {
        // Do nothing
    }
}
