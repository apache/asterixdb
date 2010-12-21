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

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class HyracksSimpleStorageManagerInterface implements IStorageManagerInterface {
    private static final long serialVersionUID = 1L;

    private static transient IBufferCache bufferCache = null;
    private static transient IFileMapManager fmManager;
    private int PAGE_SIZE = 8192;
    private int NUM_PAGES = 40;

    public HyracksSimpleStorageManagerInterface() {
        init();
    }

    public HyracksSimpleStorageManagerInterface(int pageSize, int numPages) {
        PAGE_SIZE = pageSize;
        NUM_PAGES = numPages;
        init();
    }

    private void init() {
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        fmManager = new IFileMapManager() {
            private Map<Integer, String> id2nameMap = new HashMap<Integer, String>();
            private Map<String, Integer> name2IdMap = new HashMap<String, Integer>();
            private int idCounter = 0;

            @Override
            public String lookupFileName(int fileId) throws HyracksDataException {
                String fName = id2nameMap.get(fileId);
                if (fName == null) {
                    throw new HyracksDataException("No mapping found for id: " + fileId);
                }
                return fName;
            }

            @Override
            public int lookupFileId(String fileName) throws HyracksDataException {
                Integer fileId = name2IdMap.get(fileName);
                if (fileId == null) {
                    throw new HyracksDataException("No mapping found for name: " + fileName);
                }
                return fileId;
            }

            @Override
            public boolean isMapped(String fileName) {
                return name2IdMap.containsKey(fileName);
            }

            @Override
            public boolean isMapped(int fileId) {
                return id2nameMap.containsKey(fileId);
            }

            @Override
            public void unregisterFile(int fileId) throws HyracksDataException {
                String fileName = id2nameMap.remove(fileId);
                name2IdMap.remove(fileName);
            }

            @Override
            public void registerFile(String fileName) throws HyracksDataException {
                Integer fileId = idCounter++;
                id2nameMap.put(fileId, fileName);
                name2IdMap.put(fileName, fileId);
            }
        };
        bufferCache = new BufferCache(allocator, prs, fmManager, PAGE_SIZE, NUM_PAGES);
    }

    public IFileMapProvider getFileMapProvider() {
        return fmManager;
    }

    @Override
    public IBufferCache getBufferCache() {
        return bufferCache;
    }
}
