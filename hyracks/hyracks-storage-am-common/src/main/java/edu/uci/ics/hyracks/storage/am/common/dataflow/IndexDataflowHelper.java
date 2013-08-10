/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.io.File;
import java.io.IOException;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.util.IndexFileNameUtil;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactory;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public abstract class IndexDataflowHelper implements IIndexDataflowHelper {

    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;
    protected final IIndexLifecycleManager lcManager;
    protected final ILocalResourceRepository localResourceRepository;
    protected final ResourceIdFactory resourceIdFactory;
    protected final FileReference file;
    protected final int partition;
    protected final int ioDeviceId;

    protected IIndex index;

    public IndexDataflowHelper(IIndexOperatorDescriptor opDesc, final IHyracksTaskContext ctx, int partition) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.lcManager = opDesc.getLifecycleManagerProvider().getLifecycleManager(ctx);
        this.localResourceRepository = opDesc.getStorageManager().getLocalResourceRepository(ctx);
        this.resourceIdFactory = opDesc.getStorageManager().getResourceIdFactory(ctx);
        this.partition = partition;
        this.ioDeviceId = opDesc.getFileSplitProvider().getFileSplits()[partition].getIODeviceId();
        this.file = new FileReference(new File(IndexFileNameUtil.prepareFileName(opDesc.getFileSplitProvider()
                .getFileSplits()[partition].getLocalFile().getFile().getPath(), ioDeviceId)));
    }

    protected abstract IIndex createIndexInstance() throws HyracksDataException;

    public IIndex getIndexInstance() {
        return index;
    }

    public void create() throws HyracksDataException {
        synchronized (lcManager) {
            long resourceID = getResourceID();
            index = lcManager.getIndex(resourceID);
            if (index != null) {
                lcManager.unregister(resourceID);
            } else {
                index = createIndexInstance();
            }

            // The previous resource ID needs to be removed since calling IIndex.create() may possibly destroy 
            // any physical artifact that the LocalResourceRepository is managing (e.g. a file containing the resource ID). 
            // Once the index has been created, a new resource ID can be generated.
            if (resourceID != -1) {
                localResourceRepository.deleteResourceByName(file.getFile().getPath());
            }
            index.create();
            try {
                //TODO Create LocalResource through LocalResourceFactory interface
                resourceID = resourceIdFactory.createId();
                ILocalResourceFactory localResourceFactory = opDesc.getLocalResourceFactoryProvider()
                        .getLocalResourceFactory();
                localResourceRepository.insert(localResourceFactory.createLocalResource(resourceID, file.getFile()
                        .getPath(), partition));
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            lcManager.register(resourceID, index);
        }
    }

    public void open() throws HyracksDataException {
        synchronized (lcManager) {
            long resourceID = getResourceID();

            if (resourceID == -1) {
                throw new HyracksDataException("Index does not have a valid resource ID. Has it been created yet?");
            }

            index = lcManager.getIndex(resourceID);
            if (index == null) {
                index = createIndexInstance();
                lcManager.register(resourceID, index);
            }
            lcManager.open(resourceID);
        }
    }

    public void close() throws HyracksDataException {
        synchronized (lcManager) {
            lcManager.close(getResourceID());
        }
    }

    public void destroy() throws HyracksDataException {
        synchronized (lcManager) {
            long resourceID = getResourceID();
            index = lcManager.getIndex(resourceID);
            if (index != null) {
                lcManager.unregister(resourceID);
            } else {
                index = createIndexInstance();
            }

            if (resourceID != -1) {
                localResourceRepository.deleteResourceByName(file.getFile().getPath());
            }
            index.destroy();
        }
    }

    public FileReference getFileReference() {
        return file;
    }

    public long getResourceID() throws HyracksDataException {
        LocalResource localResource = localResourceRepository.getResourceByName(file.getFile().getPath());
        if (localResource == null) {
            return -1;
        } else {
            return localResource.getResourceId();
        }
    }

    public IHyracksTaskContext getTaskContext() {
        return ctx;
    }
}