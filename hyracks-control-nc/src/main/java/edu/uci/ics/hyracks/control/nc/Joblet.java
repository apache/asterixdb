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
package edu.uci.ics.hyracks.control.nc;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.control.common.job.profiling.counters.Counter;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobletProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.StageletProfile;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.ManagedWorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;

public class Joblet implements IHyracksJobletContext, ICounterContext {
    private static final long serialVersionUID = 1L;

    private final NodeControllerService nodeController;

    private final INCApplicationContext appCtx;

    private final UUID jobId;

    private final int attempt;

    private final Map<UUID, Stagelet> stageletMap;

    private final Map<OperatorDescriptorId, Map<Integer, IOperatorEnvironment>> envMap;

    private final Map<String, Counter> counterMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    public Joblet(NodeControllerService nodeController, UUID jobId, int attempt, INCApplicationContext appCtx) {
        this.nodeController = nodeController;
        this.appCtx = appCtx;
        this.jobId = jobId;
        this.attempt = attempt;
        stageletMap = new HashMap<UUID, Stagelet>();
        envMap = new HashMap<OperatorDescriptorId, Map<Integer, IOperatorEnvironment>>();
        counterMap = new HashMap<String, Counter>();
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new ManagedWorkspaceFileFactory(this, (IOManager) appCtx.getRootContext().getIOManager());
    }

    @Override
    public UUID getJobId() {
        return jobId;
    }

    public IOperatorEnvironment getEnvironment(IOperatorDescriptor hod, int partition) {
        if (!envMap.containsKey(hod.getOperatorId())) {
            envMap.put(hod.getOperatorId(), new HashMap<Integer, IOperatorEnvironment>());
        }
        Map<Integer, IOperatorEnvironment> opEnvMap = envMap.get(hod.getOperatorId());
        if (!opEnvMap.containsKey(partition)) {
            opEnvMap.put(partition, new OperatorEnvironmentImpl());
        }
        return opEnvMap.get(partition);
    }

    private static final class OperatorEnvironmentImpl implements IOperatorEnvironment {
        private final Map<String, Object> map;

        public OperatorEnvironmentImpl() {
            map = new HashMap<String, Object>();
        }

        @Override
        public Object get(String name) {
            return map.get(name);
        }

        @Override
        public void set(String name, Object value) {
            map.put(name, value);
        }
    }

    public void setStagelet(UUID stageId, Stagelet stagelet) {
        stageletMap.put(stageId, stagelet);
    }

    public Stagelet getStagelet(UUID stageId) throws Exception {
        return stageletMap.get(stageId);
    }

    public Executor getExecutor() {
        return nodeController.getExecutor();
    }

    public synchronized void notifyStageletComplete(UUID stageId, int attempt, StageletProfile stats) throws Exception {
        stageletMap.remove(stageId);
        nodeController.notifyStageComplete(jobId, stageId, attempt, stats);
    }

    public void notifyStageletFailed(UUID stageId, int attempt) throws Exception {
        stageletMap.remove(stageId);
        nodeController.notifyStageFailed(jobId, stageId, attempt);
    }

    public NodeControllerService getNodeController() {
        return nodeController;
    }

    public synchronized void dumpProfile(JobletProfile jProfile) {
        Map<String, Long> counters = jProfile.getCounters();
        for (Map.Entry<String, Counter> e : counterMap.entrySet()) {
            counters.put(e.getKey(), e.getValue().get());
        }
        for (Stagelet si : stageletMap.values()) {
            StageletProfile sProfile = new StageletProfile(si.getStageId());
            si.dumpProfile(sProfile);
            jProfile.getStageletProfiles().put(si.getStageId(), sProfile);
        }
    }

    @Override
    public INCApplicationContext getApplicationContext() {
        return appCtx;
    }

    @Override
    public int getAttempt() {
        return attempt;
    }

    @Override
    public ICounterContext getCounterContext() {
        return this;
    }

    @Override
    public void registerDeallocatable(IDeallocatable deallocatable) {
        deallocatableRegistry.registerDeallocatable(deallocatable);
    }

    public void close() {
        deallocatableRegistry.close();
    }

    @Override
    public ByteBuffer allocateFrame() {
        return appCtx.getRootContext().allocateFrame();
    }

    @Override
    public int getFrameSize() {
        return appCtx.getRootContext().getFrameSize();
    }

    @Override
    public IIOManager getIOManager() {
        return appCtx.getRootContext().getIOManager();
    }

    @Override
    public FileReference createWorkspaceFile(String prefix) throws HyracksDataException {
        return fileFactory.createWorkspaceFile(prefix);
    }

    public Map<UUID, Stagelet> getStageletMap() {
        return stageletMap;
    }

    @Override
    public synchronized ICounter getCounter(String name, boolean create) {
        Counter counter = counterMap.get(name);
        if (counter == null && create) {
            counter = new Counter(name);
            counterMap.put(name, counter);
        }
        return counter;
    }
}