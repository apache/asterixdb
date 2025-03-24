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

package org.apache.hyracks.storage.am.common.dataflow;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.util.annotations.NotThreadSafe;

@NotThreadSafe
public class IndexDataflowHelper implements IIndexDataflowHelper {

    private final IResourceLifecycleManager<IIndex> lcManager;
    private final ILocalResourceRepository localResourceRepository;
    private final FileReference resourceRef;
    private IIndex index;

    public IndexDataflowHelper(final INCServiceContext ctx, IStorageManager storageMgr, FileReference resourceRef) {
        this.lcManager = storageMgr.getLifecycleManager(ctx);
        this.localResourceRepository = storageMgr.getLocalResourceRepository(ctx);
        this.resourceRef = resourceRef;
    }

    @Override
    public IIndex getIndexInstance() {
        return index;
    }

    @Override
    public void open() throws HyracksDataException {
        index = lcManager.registerIfAbsent(resourceRef.getRelativePath(), index);
        lcManager.open(resourceRef.getRelativePath());
    }

    @Override
    public void close() throws HyracksDataException {
        lcManager.close(resourceRef.getRelativePath());
    }

    @Override
    public void destroy() throws HyracksDataException {
        lcManager.destroy(resourceRef.getRelativePath());
    }

    @Override
    public LocalResource getResource() throws HyracksDataException {
        return localResourceRepository.get(resourceRef.getRelativePath());
    }
}
