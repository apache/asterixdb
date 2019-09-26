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
package org.apache.hyracks.storage.am.common.impls;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;

public class IndexAccessParameters implements IIndexAccessParameters {

    protected final IModificationOperationCallback modificationCallback;
    protected final ISearchOperationCallback searchOperationCallback;
    // This map is used to put additional parameters to an index accessor.
    protected Map<String, Object> paramMap = null;

    public IndexAccessParameters(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchOperationCallback) {
        this.modificationCallback = modificationCallback;
        this.searchOperationCallback = searchOperationCallback;
    }

    @Override
    public IModificationOperationCallback getModificationCallback() {
        return modificationCallback;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchOperationCallback;
    }

    @Override
    public Map<String, Object> getParameters() {
        if (paramMap == null) {
            paramMap = new HashMap<String, Object>();
        }
        return paramMap;
    }

    public static IIndexAccessParameters createNoOpParams(IIndexCursorStats stats) {
        if (stats == NoOpIndexCursorStats.INSTANCE) {
            return NoOpIndexAccessParameters.INSTANCE;
        } else {
            IndexAccessParameters iap =
                    new IndexAccessParameters(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            iap.getParameters().put(HyracksConstants.INDEX_CURSOR_STATS, stats);
            return iap;
        }
    }

}
