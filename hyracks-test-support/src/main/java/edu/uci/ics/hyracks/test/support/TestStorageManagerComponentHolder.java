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

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;
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
    private static IndexRegistry<BTree> btreeRegistry;

    private static int pageSize;
    private static int numPages;
    private static int maxOpenFiles;

    public static void init(int pageSize, int numPages, int maxOpenFiles) {
        TestStorageManagerComponentHolder.pageSize = pageSize;
        TestStorageManagerComponentHolder.numPages = numPages;
        TestStorageManagerComponentHolder.maxOpenFiles = maxOpenFiles;
        bufferCache = null;
        fileMapProvider = null;
        btreeRegistry = null;
    }

    public synchronized static IBufferCache getBufferCache(IHyracksStageletContext ctx) {
        if (bufferCache == null) {
            ICacheMemoryAllocator allocator = new HeapBufferAllocator();
            IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
            IFileMapProvider fileMapProvider = getFileMapProvider(ctx);
            bufferCache = new BufferCache(ctx.getIOManager(), allocator, prs, (IFileMapManager) fileMapProvider,
                    pageSize, numPages, maxOpenFiles);
        }
        return bufferCache;
    }

    public synchronized static IFileMapProvider getFileMapProvider(IHyracksStageletContext ctx) {
        if (fileMapProvider == null) {
            fileMapProvider = new TransientFileMapManager();
        }
        return fileMapProvider;
    }

    public synchronized static IndexRegistry<BTree> getBTreeRegistry(IHyracksStageletContext ctx) {
        if (btreeRegistry == null) {
            btreeRegistry = new IndexRegistry<BTree>();
        }
        return btreeRegistry;
    }
}