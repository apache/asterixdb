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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;

public class LSMRTreeBulkLoadContext implements IIndexBulkLoadContext {
    private final RTree rtree;
    private final BTree btree;
    private IIndexBulkLoadContext bulkLoadCtx;

    public LSMRTreeBulkLoadContext(RTree rtree, BTree btree) {
        this.rtree = rtree;
        this.btree = btree;
    }

    public void beginBulkLoad(float fillFactor) throws HyracksDataException, TreeIndexException {
        bulkLoadCtx = rtree.beginBulkLoad(fillFactor);
    }

    public RTree getRTree() {
        return rtree;
    }

    public BTree getBTree() {
        return btree;
    }

    public IIndexBulkLoadContext getBulkLoadCtx() {
        return bulkLoadCtx;
    }
}