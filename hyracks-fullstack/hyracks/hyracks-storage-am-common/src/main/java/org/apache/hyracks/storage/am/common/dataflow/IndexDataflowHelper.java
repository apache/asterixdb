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
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.IResourceLifecycleManager;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrame;
import org.apache.hyracks.storage.am.common.util.IndexFileNameUtil;
import org.apache.hyracks.storage.common.file.ILocalResourceFactory;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;
import org.apache.hyracks.storage.common.file.LocalResource;

public abstract class IndexDataflowHelper implements IIndexDataflowHelper {

    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;
    protected final IResourceLifecycleManager<IIndex> lcManager;
    protected final ILocalResourceRepository localResourceRepository;
    protected final IResourceIdFactory resourceIdFactory;
    protected final FileReference file;
    protected final int partition;
    protected final boolean durable;
    protected IIndex index;
    protected final String resourcePath;
    protected final int resourcePartition;

    public IndexDataflowHelper(IIndexOperatorDescriptor opDesc, final IHyracksTaskContext ctx, int partition,
            boolean durable) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.lcManager = opDesc.getLifecycleManagerProvider().getLifecycleManager(ctx);
        this.localResourceRepository = opDesc.getStorageManager().getLocalResourceRepository(ctx);
        this.resourceIdFactory = opDesc.getStorageManager().getResourceIdFactory(ctx);
        this.partition = partition;
        this.file = IndexFileNameUtil.getIndexAbsoluteFileRef(opDesc, partition, ctx.getIOManager());
        this.resourcePath = file.getFile().getPath();
        this.durable = durable;
        this.resourcePartition = opDesc.getFileSplitProvider().getFileSplits()[partition].getPartition();
    }

    protected abstract IIndex createIndexInstance() throws HyracksDataException;

    @Override
    public IIndex getIndexInstance() {
        return index;
    }

    @Override
    public void create() throws HyracksDataException {
        synchronized (lcManager) {
            index = lcManager.get(resourcePath);
            if (index != null) {
                //how is this right?????????? <needs to be fixed>
                lcManager.unregister(resourcePath);
            } else {
                index = createIndexInstance();
            }

            // The previous resource ID needs to be removed since calling IIndex.create() may possibly destroy
            // any physical artifact that the LocalResourceRepository is managing (e.g. a file containing the resource ID).
            // Once the index has been created, a new resource ID can be generated.
            long resourceID = getResourceID();
            if (resourceID != -1) {
                localResourceRepository.deleteResourceByPath(resourcePath);
            }
            index.create();
            try {
                resourceID = resourceIdFactory.createId();
                ILocalResourceFactory localResourceFactory = opDesc.getLocalResourceFactoryProvider()
                        .getLocalResourceFactory();
                String resourceName = opDesc.getFileSplitProvider().getFileSplits()[partition].getLocalFile().getFile()
                        .getPath();
                localResourceRepository.insert(localResourceFactory.createLocalResource(resourceID, resourceName,
                        resourcePartition, LIFOMetaDataFrame.VERSION, resourcePath));
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            lcManager.register(resourcePath, index);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        synchronized (lcManager) {
            if (getResourceID() == -1) {
                throw new HyracksDataException("Index does not have a valid resource ID. Has it been created yet?");
            }

            index = lcManager.get(resourcePath);
            if (index == null) {
                index = createIndexInstance();
                lcManager.register(resourcePath, index);
            }
            lcManager.open(resourcePath);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        synchronized (lcManager) {
            lcManager.close(resourcePath);
        }
    }

    @Override
    public void destroy() throws HyracksDataException {
        synchronized (lcManager) {
            index = lcManager.get(resourcePath);
            if (index != null) {
                lcManager.unregister(resourcePath);
            } else {
                index = createIndexInstance();
            }

            if (getResourceID() != -1) {
                localResourceRepository.deleteResourceByPath(resourcePath);
            }
            index.destroy();
        }
    }

    @Override
    public FileReference getFileReference() {
        return file;
    }

    @Override
    public long getResourceID() throws HyracksDataException {
        LocalResource lr = localResourceRepository.getResourceByPath(resourcePath);
        return lr == null ? -1 : lr.getResourceId();
    }

    @Override
    public IHyracksTaskContext getTaskContext() {
        return ctx;
    }

    @Override
    public String getResourcePath() {
        return resourcePath;
    }

    @Override
    public int getResourcePartition() {
        return resourcePartition;
    }
}
