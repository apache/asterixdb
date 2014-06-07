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

import org.apache.hadoop.mapreduce.TaskAttemptContext;

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
import edu.uci.ics.pregelix.api.graph.VertexContext;

public class RuntimeContext implements IWorkspaceFileFactory {

    private final static int SHUTDOWN_GRACEFUL_PERIOD = 5000;
    private final IIndexLifecycleManager lcManager;
    private final ILocalResourceRepository localResourceRepository;
    private final ResourceIdFactory resourceIdFactory;
    private final IBufferCache bufferCache;
    private final List<IVirtualBufferCache> vbcs;
    private final IFileMapManager fileMapManager;
    private final IOManager ioManager;
    private final Map<String, PJobContext> activeJobs = new ConcurrentHashMap<String, PJobContext>();

    private final ThreadFactory threadFactory = new ThreadFactory() {
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    };

    public RuntimeContext(INCApplicationContext appCtx, int vFrameSize) {
        int pageSize = vFrameSize;
        long memSize = Runtime.getRuntime().maxMemory();
        long bufferSize = memSize / 4;
        int numPages = (int) (bufferSize / pageSize);

        fileMapManager = new TransientFileMapManager();
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy(allocator, pageSize, numPages);
        /** let the buffer cache never flush dirty pages */
        bufferCache = new BufferCache(appCtx.getRootContext().getIOManager(), prs, new PreDelayPageCleanerPolicy(
                Long.MAX_VALUE), fileMapManager, 1000000, threadFactory);
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

    public synchronized void close() throws HyracksDataException {
        for (Entry<String, PJobContext> entry : activeJobs.entrySet()) {
            entry.getValue().close();
        }
        activeJobs.clear();
        // wait a graceful period until all active operators using tree cursors are dead
        try {
            wait(SHUTDOWN_GRACEFUL_PERIOD);
        } catch (InterruptedException e) {

        }
        bufferCache.close();
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

    public synchronized Map<TaskIterationID, IStateObject> getAppStateStore(String jobId) {
        PJobContext activeJob = getActiveJob(jobId);
        return activeJob.getAppStateStore();
    }

    public static RuntimeContext get(IHyracksTaskContext ctx) {
        return (RuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject();
    }

    public synchronized void setVertexProperties(String jobId, long numVertices, long numEdges, long currentIteration,
            ClassLoader cl) {
        PJobContext activeJob = getOrCreateActiveJob(jobId);
        activeJob.setVertexProperties(numVertices, numEdges, currentIteration, cl);
    }

    public synchronized void recoverVertexProperties(String jobId, long numVertices, long numEdges,
            long currentIteration, ClassLoader cl) {
        PJobContext activeJob = getActiveJob(jobId);
        activeJob.recoverVertexProperties(numVertices, numEdges, currentIteration, cl);
    }

    public synchronized void endSuperStep(String jobId) {
        PJobContext activeJob = getActiveJob(jobId);
        activeJob.endSuperStep();
    }

    public synchronized void clearState(String jobId, boolean allStates) throws HyracksDataException {
        PJobContext activeJob = getActiveJob(jobId);
        if (activeJob != null) {
            activeJob.clearState();
            if (allStates) {
                activeJobs.remove(jobId);
            }
        }
    }

    public long getSuperstep(String jobId) {
        PJobContext activeJob = getActiveJob(jobId);
        return activeJob == null ? 0 : activeJob.getVertexContext().getSuperstep();
    }

    public void setJobContext(String jobId, TaskAttemptContext tCtx) {
        PJobContext activeJob = getOrCreateActiveJob(jobId);
        activeJob.getVertexContext().setContext(tCtx);
    }

    public VertexContext getVertexContext(String jobId) {
        PJobContext activeJob = getActiveJob(jobId);
        return activeJob.getVertexContext();
    }

    private PJobContext getActiveJob(String jobId) {
        PJobContext activeJob = activeJobs.get(jobId);
        return activeJob;
    }

    private PJobContext getOrCreateActiveJob(String jobId) {
        PJobContext activeJob = activeJobs.get(jobId);
        if (activeJob == null) {
            activeJob = new PJobContext();
            activeJobs.put(jobId, activeJob);
        }
        return activeJob;
    }

    @Override
    public FileReference createManagedWorkspaceFile(String jobId) throws HyracksDataException {
        final FileReference fRef = ioManager.createWorkspaceFile(jobId);
        PJobContext activeJob = getActiveJob(jobId);
        long superstep = activeJob.getVertexContext().getSuperstep();
        List<FileReference> files = activeJob.getIterationToFiles().get(superstep);
        if (files == null) {
            files = new ArrayList<FileReference>();
            activeJob.getIterationToFiles().put(superstep, files);
        }
        files.add(fRef);
        return fRef;
    }

    @Override
    public FileReference createUnmanagedWorkspaceFile(String prefix) throws HyracksDataException {
        return ioManager.createWorkspaceFile(prefix);
    }

}