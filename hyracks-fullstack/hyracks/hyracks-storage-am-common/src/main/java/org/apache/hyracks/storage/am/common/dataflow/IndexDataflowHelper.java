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

import java.io.IOException;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.IResourceLifecycleManager;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrame;
import org.apache.hyracks.storage.common.file.ILocalResourceFactory;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;
import org.apache.hyracks.storage.common.file.LocalResource;

public abstract class IndexDataflowHelper implements IIndexDataflowHelper {

    protected final IHyracksTaskContext ctx;
    protected final IIOManager ioManager;
    protected final IIndexOperatorDescriptor opDesc;
    protected final IResourceLifecycleManager<IIndex> lcManager;
    protected final ILocalResourceRepository localResourceRepository;
    protected final IResourceIdFactory resourceIdFactory;
    protected final boolean durable;
    protected final FileReference resourceRef;
    protected final String resourceName;
    protected final int partition;
    protected IIndex index;

    public IndexDataflowHelper(IIndexOperatorDescriptor opDesc, final IHyracksTaskContext ctx, int partition,
            boolean durable) throws HyracksDataException {
        this.ctx = ctx;
        this.opDesc = opDesc;
        this.ioManager = ctx.getIOManager();
        this.lcManager = opDesc.getLifecycleManagerProvider().getLifecycleManager(ctx);
        this.localResourceRepository = opDesc.getStorageManager().getLocalResourceRepository(ctx);
        this.resourceIdFactory = opDesc.getStorageManager().getResourceIdFactory(ctx);
        FileSplit fileSplit = opDesc.getFileSplitProvider().getFileSplits()[partition];
        this.resourceRef = ioManager.getFileRef(fileSplit.getPath(), fileSplit.isManaged());
        this.resourceName = resourceRef.getRelativePath();
        this.durable = durable;
        this.partition = partition;
    }

    protected abstract IIndex createIndexInstance() throws HyracksDataException;

    @Override
    public IIndex getIndexInstance() {
        return index;
    }

    @Override
    public void create() throws HyracksDataException {
        synchronized (lcManager) {
            index = lcManager.get(resourceRef.getRelativePath());
            if (index != null) {
                //how is this right?????????? <needs to be fixed>
                lcManager.unregister(resourceRef.getRelativePath());
            } else {
                index = createIndexInstance();
            }

            // The previous resource ID needs to be removed since calling IIndex.create() may possibly destroy
            // any physical artifact that the LocalResourceRepository is managing (e.g. a file containing the resource ID).
            // Once the index has been created, a new resource ID can be generated.
            long resourceID = getResourceID();
            if (resourceID != -1) {
                localResourceRepository.delete(resourceRef.getRelativePath());
            }
            index.create();
            try {
                resourceID = resourceIdFactory.createId();
                ILocalResourceFactory localResourceFactory = opDesc.getLocalResourceFactoryProvider()
                        .getLocalResourceFactory();
                localResourceRepository.insert(localResourceFactory.createLocalResource(resourceID, resourceRef
                        .getRelativePath(),
                        LIFOMetaDataFrame.VERSION, partition));
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            lcManager.register(resourceRef.getRelativePath(), index);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        synchronized (lcManager) {
            if (getResourceID() == -1) {
                throw new HyracksDataException("Index does not have a valid resource ID. Has it been created yet?");
            }

            index = lcManager.get(resourceRef.getRelativePath());
            if (index == null) {
                index = createIndexInstance();
                lcManager.register(resourceRef.getRelativePath(), index);
            }
            lcManager.open(resourceRef.getRelativePath());
        }
    }

    @Override
    public void close() throws HyracksDataException {
        synchronized (lcManager) {
            lcManager.close(resourceRef.getRelativePath());
        }
    }

    @Override
    public void destroy() throws HyracksDataException {
        synchronized (lcManager) {
            index = lcManager.get(resourceRef.getRelativePath());
            if (index != null) {
                lcManager.unregister(resourceRef.getRelativePath());
            } else {
                index = createIndexInstance();
            }

            if (getResourceID() != -1) {
                localResourceRepository.delete(resourceRef.getRelativePath());
            }
            index.destroy();
        }
    }

    private long getResourceID() throws HyracksDataException {
        LocalResource lr = localResourceRepository.get(resourceRef.getRelativePath());
        return lr == null ? -1 : lr.getId();
    }

    @Override
    public LocalResource getResource() throws HyracksDataException {
        return localResourceRepository.get(resourceRef.getRelativePath());
    }
}
