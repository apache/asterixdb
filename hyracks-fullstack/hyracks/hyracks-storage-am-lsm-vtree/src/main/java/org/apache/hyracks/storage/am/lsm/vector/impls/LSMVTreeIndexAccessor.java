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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;

/**
 * LSM Vector Clustering Tree Index Accessor.
 *
 * <p>Default search routes through the streaming {@link LSMVTreeSearchCursor} (registered on the
 * parent accessor as the {@code cursorFactory}). Production ANN queries opt in to the quantized
 * top-K cursor by setting {@link LSMVTreeTopKSearchCursor#IAP_KEY} to {@code Boolean.TRUE} in the
 * index-access parameters; the test fixtures that don't set the flag get the streaming cursor,
 * which is the same cursor used by component merges.
 */
public class LSMVTreeIndexAccessor extends LSMTreeIndexAccessor {

    private final LSMVTree lsmVTree;

    public LSMVTreeIndexAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx, ICursorFactory cursorFactory,
            LSMVTree lsmVTree) {
        super(lsmHarness, ctx, cursorFactory);
        this.lsmVTree = lsmVTree;
    }

    @Override
    public IIndexCursor createSearchCursor(boolean exclusive) {
        if (ctx instanceof LSMVTreeOpContext) {
            IIndexAccessParameters iap = ((LSMVTreeOpContext) ctx).getIndexAccessParameters();
            if (iap != null) {
                Boolean useTopK = iap.getParameter(LSMVTreeTopKSearchCursor.IAP_KEY, Boolean.class);
                if (Boolean.TRUE.equals(useTopK)) {
                    return lsmVTree.createTopKSearchCursor(ctx);
                }
            }
        }
        return super.createSearchCursor(exclusive);
    }
}
