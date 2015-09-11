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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;

/**
 * Dummy NoOp callback factory used primarily for testing. Always returns the {@link NoOpOperationCallback} instance.
 * Implemented as an enum to preserve singleton model while being serializable
 */
public enum NoOpOperationCallbackFactory implements ISearchOperationCallbackFactory,
        IModificationOperationCallbackFactory {
    INSTANCE;

    @Override
    public IModificationOperationCallback createModificationOperationCallback(long resourceId, Object resource, IHyracksTaskContext ctx) {
        return NoOpOperationCallback.INSTANCE;
    }

    @Override
    public ISearchOperationCallback createSearchOperationCallback(long resourceId, IHyracksTaskContext ctx) {
        return NoOpOperationCallback.INSTANCE;
    }
}
