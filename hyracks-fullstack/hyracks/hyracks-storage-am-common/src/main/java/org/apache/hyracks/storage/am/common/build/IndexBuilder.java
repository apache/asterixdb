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
package org.apache.hyracks.storage.am.common.build;

import java.io.IOException;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.storage.am.common.api.IIndexBuilder;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexBuilder implements IIndexBuilder {
    private static final Logger LOGGER = LogManager.getLogger();

    protected final INCServiceContext ctx;
    protected final IStorageManager storageManager;
    protected final FileReference resourceRef;
    protected final IResourceFactory localResourceFactory;
    protected final boolean durable;
    private final IResourceIdFactory resourceIdFactory;

    /*
     * Ideally, we should not pass resource id factory to the constructor since we can obtain it through
     * storageManager.getResourceIdFactory(ctx). However, in some cases, we want to create some resources
     * with specific resource ids. See MetadataBootstrap
     */
    public IndexBuilder(INCServiceContext ctx, IStorageManager storageManager, IResourceIdFactory resourceIdFactory,
            FileReference resourceRef, IResourceFactory localResourceFactory, boolean durable)
            throws HyracksDataException {
        this.ctx = ctx;
        this.storageManager = storageManager;
        this.resourceIdFactory = resourceIdFactory;
        this.localResourceFactory = localResourceFactory;
        this.durable = durable;
        this.resourceRef = resourceRef;
    }

    @Override
    public void build() throws HyracksDataException {
        IResourceLifecycleManager<IIndex> lcManager = storageManager.getLifecycleManager(ctx);
        synchronized (lcManager) {
            // The previous resource Id needs to be removed since calling IIndex.create() may possibly destroy any
            // physical artifact that the LocalResourceRepository is managing (e.g. a file containing the resource Id).
            // Once the index has been created, a new resource Id can be generated.
            ILocalResourceRepository localResourceRepository = storageManager.getLocalResourceRepository(ctx);
            LocalResource lr = localResourceRepository.get(resourceRef.getRelativePath());
            long resourceId = lr == null ? -1 : lr.getId();
            if (resourceId != -1) {
                localResourceRepository.delete(resourceRef.getRelativePath());
            }
            resourceId = resourceIdFactory.createId();
            IResource resource = localResourceFactory.createResource(resourceRef);
            lr = new LocalResource(resourceId, ITreeIndexFrame.Constants.VERSION, durable, resource);
            IIndex index = lcManager.get(resourceRef.getRelativePath());
            if (index != null) {
                //how is this right?????????? <needs to be fixed>
                //The reason for this is to handle many cases such as:
                //1. Crash while delete index is running (we don't do global cleanup on restart)
                //2. Node leaves and then join with old data
                LOGGER.log(Level.WARN,
                        "Removing existing index on index create for the index: " + resourceRef.getRelativePath());
                lcManager.unregister(resourceRef.getRelativePath());
                index.destroy();
            } else {
                if (resourceRef.getFile().exists()) {
                    // Index is not registered but the index file exists
                    // This is another big problem that we need to disallow soon
                    // We can only disallow this if we have a global cleanup after crash
                    // on reboot
                    LOGGER.log(Level.WARN,
                            "Deleting " + resourceRef.getRelativePath()
                                    + " on index create. The index is not registered"
                                    + " but the file exists in the filesystem");
                    IoUtil.delete(resourceRef);
                }
                index = resource.createInstance(ctx);
            }
            index.create();
            try {
                localResourceRepository.insert(lr);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            lcManager.register(resourceRef.getRelativePath(), index);
        }
    }
}
