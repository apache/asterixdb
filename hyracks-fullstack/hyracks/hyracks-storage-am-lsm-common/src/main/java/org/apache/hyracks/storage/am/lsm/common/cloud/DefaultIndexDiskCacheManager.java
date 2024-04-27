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
package org.apache.hyracks.storage.am.lsm.common.cloud;

import org.apache.hyracks.storage.common.disk.ISweepContext;

public final class DefaultIndexDiskCacheManager implements IIndexDiskCacheManager {
    public static final IIndexDiskCacheManager INSTANCE = new DefaultIndexDiskCacheManager();
    private static final String NOT_SWEEPABLE_ERR_MSG = "Index is not sweepable";

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public boolean isSweepable() {
        return false;
    }

    @Override
    public boolean prepareSweepPlan() {
        throw new IllegalStateException(NOT_SWEEPABLE_ERR_MSG);
    }

    @Override
    public long sweep(ISweepContext context) {
        throw new IllegalStateException(NOT_SWEEPABLE_ERR_MSG);
    }
}
