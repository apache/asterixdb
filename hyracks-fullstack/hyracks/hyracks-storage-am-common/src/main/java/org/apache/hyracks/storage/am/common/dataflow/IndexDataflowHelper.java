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
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexDataflowHelper implements IIndexDataflowHelper {

    private static final Logger LOGGER = LogManager.getLogger();
    private final INCServiceContext ctx;
    private final IResourceLifecycleManager<IIndex> lcManager;
    private final ILocalResourceRepository localResourceRepository;
    private final FileReference resourceRef;
    private IIndex index;

    public IndexDataflowHelper(final INCServiceContext ctx, IStorageManager storageMgr, FileReference resourceRef)
            throws HyracksDataException {
        this.ctx = ctx;
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
        //Get local resource file
        synchronized (lcManager) {
            index = lcManager.get(resourceRef.getRelativePath());
            if (index == null) {
                LocalResource lr = readIndex();
                lcManager.register(lr.getPath(), index);
            }
            lcManager.open(resourceRef.getRelativePath());
        }
    }

    private LocalResource readIndex() throws HyracksDataException {
        // Get local resource
        LocalResource lr = getResource();
        if (lr == null) {
            throw HyracksDataException.create(ErrorCode.INDEX_DOES_NOT_EXIST);
        }
        IResource resource = lr.getResource();
        index = resource.createInstance(ctx);
        return lr;
    }

    @Override
    public void close() throws HyracksDataException {
        synchronized (lcManager) {
            lcManager.close(resourceRef.getRelativePath());
        }
    }

    @Override
    public void destroy() throws HyracksDataException {
        LOGGER.log(Level.INFO, "Dropping index " + resourceRef.getRelativePath() + " on node " + ctx.getNodeId());
        synchronized (lcManager) {
            index = lcManager.get(resourceRef.getRelativePath());
            if (index != null) {
                lcManager.unregister(resourceRef.getRelativePath());
            } else {
                readIndex();
            }

            if (getResourceId() != -1) {
                localResourceRepository.delete(resourceRef.getRelativePath());
            }
            index.destroy();
        }
    }

    private long getResourceId() throws HyracksDataException {
        LocalResource lr = localResourceRepository.get(resourceRef.getRelativePath());
        return lr == null ? -1 : lr.getId();
    }

    @Override
    public LocalResource getResource() throws HyracksDataException {
        return localResourceRepository.get(resourceRef.getRelativePath());
    }
}
