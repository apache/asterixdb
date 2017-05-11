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

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.impls.AbstractSearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

public class SearchPredicate extends AbstractSearchPredicate {

    private static final long serialVersionUID = 1L;

    protected ITupleReference searchKey;
    protected MultiComparator cmp;

    public SearchPredicate(ITupleReference searchKey, MultiComparator cmp) {
        this.searchKey = searchKey;
        this.cmp = cmp;
    }

    public SearchPredicate(ITupleReference searchKey, MultiComparator cmp, ITupleReference minFilterTuple,
            ITupleReference maxFilterTuple) {
        super(minFilterTuple, maxFilterTuple);
        this.searchKey = searchKey;
        this.cmp = cmp;
    }

    @Override
    public ITupleReference getLowKey() {
        return searchKey;
    }

    public void setSearchKey(ITupleReference searchKey) {
        this.searchKey = searchKey;
    }

    @Override
    public MultiComparator getLowKeyComparator() {
        return cmp;
    }

    @Override
    public MultiComparator getHighKeyComparator() {
        return cmp;
    }
}
