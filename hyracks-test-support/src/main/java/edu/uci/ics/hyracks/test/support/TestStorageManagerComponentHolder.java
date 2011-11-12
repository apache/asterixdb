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
package edu.uci.ics.hyracks.test.support;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.smi.TransientFileMapManager;

public class TestStorageManagerComponentHolder {
    private static IBufferCache bufferCache;
    private static IFileMapProvider fileMapProvider;
    private static IndexRegistry<ITreeIndex> treeIndexRegistry;
    private static IndexRegistry<InvertedIndex> invIndexRegistry;

    private static int pageSize;
    private static int numPages;
    private static int maxOpenFiles;

    public static void init(int pageSize, int numPages, int maxOpenFiles) {
        TestStorageManagerComponentHolder.pageSize = pageSize;
        TestStorageManagerComponentHolder.numPages = numPages;
        TestStorageManagerComponentHolder.maxOpenFiles = maxOpenFiles;
        bufferCache = null;
        fileMapProvider = null;
        treeIndexRegistry = null;
        invIndexRegistry = null;
    }

    public synchronized static IBufferCache getBufferCache(IHyracksTaskContext ctx) {
        if (bufferCache == null) {
            ICacheMemoryAllocator allocator = new HeapBufferAllocator();
            IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
            IFileMapProvider fileMapProvider = getFileMapProvider(ctx);
            bufferCache = new BufferCache(ctx.getIOManager(), allocator, prs, (IFileMapManager) fileMapProvider,
                    pageSize, numPages, maxOpenFiles);
        }
        return bufferCache;
    }

    public synchronized static IFileMapProvider getFileMapProvider(IHyracksTaskContext ctx) {
        if (fileMapProvider == null) {
            fileMapProvider = new TransientFileMapManager();
        }
        return fileMapProvider;
    }

    public synchronized static IndexRegistry<ITreeIndex> getTreeIndexRegistry(IHyracksTaskContext ctx) {
        if (treeIndexRegistry == null) {
            treeIndexRegistry = new IndexRegistry<ITreeIndex>();
        }
        return treeIndexRegistry;
    }
    
    public synchronized static IndexRegistry<InvertedIndex> getInvertedIndexRegistry(IHyracksTaskContext ctx) {
        if (invIndexRegistry == null) {
        	invIndexRegistry = new IndexRegistry<InvertedIndex>();
        }
        return invIndexRegistry;
    }
}