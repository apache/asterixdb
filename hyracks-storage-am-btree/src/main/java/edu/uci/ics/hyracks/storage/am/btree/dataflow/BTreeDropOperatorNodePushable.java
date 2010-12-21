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

package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.File;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class BTreeDropOperatorNodePushable extends AbstractOperatorNodePushable {

    private IBTreeRegistryProvider btreeRegistryProvider;
    private IStorageManagerInterface storageManager;
    private IFileSplitProvider fileSplitProvider;
    private int partition;

    public BTreeDropOperatorNodePushable(IStorageManagerInterface storageManager,
            IBTreeRegistryProvider btreeRegistryProvider, IFileSplitProvider fileSplitProvider, int partition) {
        this.storageManager = storageManager;
        this.btreeRegistryProvider = btreeRegistryProvider;
        this.fileSplitProvider = fileSplitProvider;
        this.partition = partition;
    }

    @Override
    public void deinitialize() throws HyracksDataException {
    }

    @Override
    public int getInputArity() {
        return 0;
    }

    @Override
    public IFrameWriter getInputFrameWriter(int index) {
        return null;
    }

    @Override
    public void initialize() throws HyracksDataException {

        BTreeRegistry btreeRegistry = btreeRegistryProvider.getBTreeRegistry();
        IBufferCache bufferCache = storageManager.getBufferCache();
        IFileMapProvider fileMapProvider = storageManager.getFileMapProvider();

        File f = fileSplitProvider.getFileSplits()[partition].getLocalFile();
        String fileName = f.getAbsolutePath();

        boolean fileIsMapped = fileMapProvider.isMapped(fileName);
        if (!fileIsMapped) {
            throw new HyracksDataException("Cannot drop B-Tree with name " + fileName + ". No file mapping exists.");
        }

        int btreeFileId = fileMapProvider.lookupFileId(fileName);

        // unregister btree instance
        btreeRegistry.lock();
        try {
            btreeRegistry.unregister(btreeFileId);
        } finally {
            btreeRegistry.unlock();
        }

        // remove name to id mapping
        bufferCache.deleteFile(btreeFileId);

        // TODO: should this be handled through the BufferCache or
        // FileMapProvider?
        if (f.exists()) {
            f.delete();
        }
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
    }
}
