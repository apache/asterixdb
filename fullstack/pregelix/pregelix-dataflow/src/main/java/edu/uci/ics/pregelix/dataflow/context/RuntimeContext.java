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
package edu.uci.ics.pregelix.dataflow.context;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
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
import edu.uci.ics.pregelix.api.graph.Vertex;

public class RuntimeContext implements IWorkspaceFileFactory {
    private static final Logger LOGGER = Logger.getLogger(RuntimeContext.class.getName());

    private IndexRegistry<IIndex> treeIndexRegistry;
    private IBufferCache bufferCache;
    private IFileMapManager fileMapManager;
    private Map<StateKey, IStateObject> appStateMap = new ConcurrentHashMap<StateKey, IStateObject>();
    private Map<String, Long> giraphJobIdToSuperStep = new ConcurrentHashMap<String, Long>();
    private Map<String, Boolean> giraphJobIdToMove = new ConcurrentHashMap<String, Boolean>();
    private IOManager ioManager;
    private Map<Long, List<FileReference>> iterationToFiles = new ConcurrentHashMap<Long, List<FileReference>>();

    public RuntimeContext(INCApplicationContext appCtx) {
        fileMapManager = new TransientFileMapManager();
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy();
        int pageSize = 64 * 1024;
        long memSize = Runtime.getRuntime().maxMemory();
        long bufferSize = memSize / 4;
        int numPages = (int) (bufferSize / pageSize);
        /** let the buffer cache never flush dirty pages */
        bufferCache = new BufferCache(appCtx.getRootContext().getIOManager(), allocator, prs,
                new PreDelayPageCleanerPolicy(Long.MAX_VALUE), fileMapManager, pageSize, numPages, 1000000);
        treeIndexRegistry = new IndexRegistry<IIndex>();
        ioManager = (IOManager) appCtx.getRootContext().getIOManager();
    }

    public void close() {
        for (Entry<Long, List<FileReference>> entry : iterationToFiles.entrySet())
            for (FileReference fileRef : entry.getValue())
                fileRef.delete();

        iterationToFiles.clear();
        bufferCache.close();
        appStateMap.clear();

        System.gc();
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public IFileMapProvider getFileMapManager() {
        return fileMapManager;
    }

    public IndexRegistry<IIndex> getTreeIndexRegistry() {
        return treeIndexRegistry;
    }

    public Map<StateKey, IStateObject> getAppStateStore() {
        return appStateMap;
    }

    public static RuntimeContext get(IHyracksTaskContext ctx) {
        return (RuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject();
    }

    public synchronized void setVertexProperties(String giraphJobId, long numVertices, long numEdges) {
        Boolean toMove = giraphJobIdToMove.get(giraphJobId);
        if (toMove == null || toMove == true) {
            if (giraphJobIdToSuperStep.get(giraphJobId) == null) {
                giraphJobIdToSuperStep.put(giraphJobId, 0L);
            }

            long superStep = giraphJobIdToSuperStep.get(giraphJobId);
            List<FileReference> files = iterationToFiles.remove(superStep - 1);
            if (files != null) {
                for (FileReference fileRef : files)
                    fileRef.delete();
            }

            Vertex.setSuperstep(++superStep);
            Vertex.setNumVertices(numVertices);
            Vertex.setNumEdges(numEdges);
            giraphJobIdToSuperStep.put(giraphJobId, superStep);
            giraphJobIdToMove.put(giraphJobId, false);
            LOGGER.info("start iteration " + Vertex.getCurrentSuperstep());
        }
        System.gc();
    }

    public synchronized void endSuperStep(String giraphJobId) {
        giraphJobIdToMove.put(giraphJobId, true);
        LOGGER.info("end iteration " + Vertex.getCurrentSuperstep());
    }

    @Override
    public FileReference createManagedWorkspaceFile(String prefix) throws HyracksDataException {
        final FileReference fRef = ioManager.createWorkspaceFile(prefix);
        List<FileReference> files = iterationToFiles.get(Vertex.getCurrentSuperstep());
        if (files == null) {
            files = new ArrayList<FileReference>();
            iterationToFiles.put(Vertex.getCurrentSuperstep(), files);
        }
        files.add(fRef);
        return fRef;
    }

    @Override
    public FileReference createUnmanagedWorkspaceFile(String prefix) throws HyracksDataException {
        return ioManager.createWorkspaceFile(prefix);
    }
}