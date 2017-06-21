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
package org.apache.hyracks.test.support;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.ResourceIdFactory;

public class TestStorageManager implements IStorageManager {
    private static final long serialVersionUID = 1L;

    @Override
    public IBufferCache getBufferCache(INCServiceContext ctx) {
        return TestStorageManagerComponentHolder.getBufferCache(ctx);
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository(INCServiceContext ctx) {
        return TestStorageManagerComponentHolder.getLocalResourceRepository();
    }

    @Override
    public ResourceIdFactory getResourceIdFactory(INCServiceContext ctx) {
        return TestStorageManagerComponentHolder.getResourceIdFactory();
    }

    @Override
    public IResourceLifecycleManager<IIndex> getLifecycleManager(INCServiceContext ctx) {
        return TestStorageManagerComponentHolder.getIndexLifecycleManager();
    }

}
