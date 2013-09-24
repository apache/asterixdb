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
package edu.uci.ics.pregelix.dataflow.context;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.MultitenantVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.BufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;
import edu.uci.ics.hyracks.storage.common.file.TransientLocalResourceRepository;
import edu.uci.ics.pregelix.api.graph.Vertex;

public class RuntimeContext implements IWorkspaceFileFactory {
    private static final Logger LOGGER = Logger.getLogger(RuntimeContext.class.getName());

    private final IIndexLifecycleManager lcManager;
    private final ILocalResourceRepository localResourceRepository;
    private final ResourceIdFactory resourceIdFactory;
    private final IBufferCache bufferCache;
    private final List<IVirtualBufferCache> vbcs;
    private final IFileMapManager fileMapManager;
    private final IOManager ioManager;
    private final Map<Long, List<FileReference>> iterationToFiles = new ConcurrentHashMap<Long, List<FileReference>>();
    private final Map<StateKey, IStateObject> appStateMap = new ConcurrentHashMap<StateKey, IStateObject>();
    private final Map<String, Long> jobIdToSuperStep = new ConcurrentHashMap<String, Long>();
    private final Map<String, Boolean> jobIdToMove = new ConcurrentHashMap<String, Boolean>();

    private final ThreadFactory threadFactory = new ThreadFactory() {
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    };

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
                new PreDelayPageCleanerPolicy(Long.MAX_VALUE), fileMapManager, pageSize, numPages, 1000000,
                threadFactory);
        int numPagesInMemComponents = numPages / 8;
        vbcs = new ArrayList<IVirtualBufferCache>();
        IVirtualBufferCache vBufferCache = new MultitenantVirtualBufferCache(new VirtualBufferCache(
                new HeapBufferAllocator(), pageSize, numPagesInMemComponents));
        vbcs.add(vBufferCache);
        ioManager = (IOManager) appCtx.getRootContext().getIOManager();
        lcManager = new NoBudgetIndexLifecycleManager();
        localResourceRepository = new TransientLocalResourceRepository();
        resourceIdFactory = new ResourceIdFactory(0);
    }

    public void close() throws HyracksDataException {
        for (Entry<Long, List<FileReference>> entry : iterationToFiles.entrySet())
            for (FileReference fileRef : entry.getValue())
                fileRef.delete();

        iterationToFiles.clear();
        bufferCache.close();
        appStateMap.clear();

        System.gc();
    }

    public void clearState(String jobId) throws HyracksDataException {
        for (Entry<Long, List<FileReference>> entry : iterationToFiles.entrySet())
            for (FileReference fileRef : entry.getValue())
                fileRef.delete();

        iterationToFiles.clear();
        appStateMap.clear();
        jobIdToMove.remove(jobId);
        jobIdToSuperStep.remove(jobId);
        System.gc();
    }

    public ILocalResourceRepository getLocalResourceRepository() {
        return localResourceRepository;
    }

    public ResourceIdFactory getResourceIdFactory() {
        return resourceIdFactory;
    }

    public IIndexLifecycleManager getIndexLifecycleManager() {
        return lcManager;
    }

    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public List<IVirtualBufferCache> getVirtualBufferCaches() {
        return vbcs;
    }

    public IFileMapProvider getFileMapManager() {
        return fileMapManager;
    }

    public Map<StateKey, IStateObject> getAppStateStore() {
        return appStateMap;
    }

    public static RuntimeContext get(IHyracksTaskContext ctx) {
        return (RuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject();
    }

    public synchronized void setVertexProperties(String jobId, long numVertices, long numEdges, long currentIteration) {
        Boolean toMove = jobIdToMove.get(jobId);
        if (toMove == null || toMove == true) {
            if (jobIdToSuperStep.get(jobId) == null) {
                if (currentIteration <= 0) {
                    jobIdToSuperStep.put(jobId, 0L);
                } else {
                    jobIdToSuperStep.put(jobId, currentIteration);
                }
            }

            long superStep = jobIdToSuperStep.get(jobId);
            List<FileReference> files = iterationToFiles.remove(superStep - 1);
            if (files != null) {
                for (FileReference fileRef : files)
                    fileRef.delete();
            }

            if (currentIteration > 0) {
                Vertex.setSuperstep(currentIteration);
            } else {
                Vertex.setSuperstep(++superStep);
            }
            Vertex.setNumVertices(numVertices);
            Vertex.setNumEdges(numEdges);
            jobIdToSuperStep.put(jobId, superStep);
            jobIdToMove.put(jobId, false);
            LOGGER.info("start iteration " + Vertex.getSuperstep());
        }
        System.gc();
    }

    public synchronized void recoverVertexProperties(String jobId, long numVertices, long numEdges,
            long currentIteration) {
        if (jobIdToSuperStep.get(jobId) == null) {
            if (currentIteration <= 0) {
                jobIdToSuperStep.put(jobId, 0L);
            } else {
                jobIdToSuperStep.put(jobId, currentIteration);
            }
        }

        long superStep = jobIdToSuperStep.get(jobId);
        List<FileReference> files = iterationToFiles.remove(superStep - 1);
        if (files != null) {
            for (FileReference fileRef : files)
                fileRef.delete();
        }

        if (currentIteration > 0) {
            Vertex.setSuperstep(currentIteration);
        } else {
            Vertex.setSuperstep(++superStep);
        }
        Vertex.setNumVertices(numVertices);
        Vertex.setNumEdges(numEdges);
        jobIdToSuperStep.put(jobId, superStep);
        jobIdToMove.put(jobId, true);
        LOGGER.info("recovered iteration " + Vertex.getSuperstep());
    }

    public synchronized void endSuperStep(String pregelixJobId) {
        jobIdToMove.put(pregelixJobId, true);
        LOGGER.info("end iteration " + Vertex.getSuperstep());
    }

    @Override
    public FileReference createManagedWorkspaceFile(String prefix) throws HyracksDataException {
        final FileReference fRef = ioManager.createWorkspaceFile(prefix);
        List<FileReference> files = iterationToFiles.get(Vertex.getSuperstep());
        if (files == null) {
            files = new ArrayList<FileReference>();
            iterationToFiles.put(Vertex.getSuperstep(), files);
        }
        files.add(fRef);
        return fRef;
    }

    @Override
    public FileReference createUnmanagedWorkspaceFile(String prefix) throws HyracksDataException {
        return ioManager.createWorkspaceFile(prefix);
    }
}