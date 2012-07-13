/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.common.file.IIndexArtifactMap;

public abstract class IndexDataflowHelper {
    protected final int partition;
    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;
    protected final IIndexArtifactMap indexArtifactMap;

    protected IIndex index;
    protected long resourceID = -1;

    public IndexDataflowHelper(IIndexOperatorDescriptor opDesc, final IHyracksTaskContext ctx, int partition) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.partition = partition;
        this.indexArtifactMap = opDesc.getStorageManager().getIndexArtifactMap(ctx);
    }

    public void init(boolean forceCreate) throws HyracksDataException {
        IndexRegistry<IIndex> indexRegistry = opDesc.getIndexRegistryProvider().getRegistry(ctx);
        FileReference file = getFilereference();
        List<IODeviceHandle> ioDeviceHandles = ctx.getIOManager().getIODevices();
        String fullDir = ioDeviceHandles.get(0).getPath().toString();
        if (!fullDir.endsWith(File.separator)) {
            fullDir += File.separator;
        }
        String baseDir = file.getFile().getPath();
        if (!baseDir.endsWith(File.separator)) {
            baseDir += File.separator;
        }
        fullDir += baseDir;

        resourceID = indexArtifactMap.get(fullDir);

        // Create new index instance and register it.
        synchronized (indexRegistry) {
            // Check if the index has already been registered.
            boolean register = false;
            index = indexRegistry.get(resourceID);
            if (index == null) {
                index = createIndexInstance();
                register = true;
            }
            if (forceCreate) {
                index.create();
                // Create new resourceId
                try {
                    resourceID = indexArtifactMap.create(baseDir, ioDeviceHandles);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            if (register) {
                index.activate();
                indexRegistry.register(resourceID, index);
            }
        }
    }

    public abstract IIndex createIndexInstance() throws HyracksDataException;

    public FileReference getFilereference() {
        IFileSplitProvider fileSplitProvider = opDesc.getFileSplitProvider();
        return fileSplitProvider.getFileSplits()[partition].getLocalFile();
    }

    public void deinit() throws HyracksDataException {
    }

    public IIndex getIndex() {
        return index;
    }

    public IHyracksTaskContext getHyracksTaskContext() {
        return ctx;
    }

    public IIndexOperatorDescriptor getOperatorDescriptor() {
        return opDesc;
    }

    public long getResourceID() {
        return resourceID;
    }
}
