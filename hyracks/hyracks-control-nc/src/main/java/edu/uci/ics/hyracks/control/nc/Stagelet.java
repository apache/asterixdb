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
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.OperatorInstanceId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.job.profiling.om.StageletProfile;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.control.common.job.profiling.counters.Counter;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.ManagedWorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;
import edu.uci.ics.hyracks.control.nc.runtime.OperatorRunnable;

public class Stagelet implements IHyracksStageletContext, ICounterContext {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(Stagelet.class.getName());

    private final Joblet joblet;

    private final UUID stageId;

    private final int attempt;

    private final Map<OperatorInstanceId, OperatorRunnable> honMap;

    private final Map<String, Counter> counterMap;

    private final IWorkspaceFileFactory fileFactory;

    private List<Endpoint> endpointList;

    private boolean started;

    private boolean abort;

    private final Set<OperatorInstanceId> pendingOperators;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    public Stagelet(Joblet joblet, UUID stageId, int attempt, String nodeId) throws RemoteException {
        this.joblet = joblet;
        this.stageId = stageId;
        this.attempt = attempt;
        pendingOperators = new HashSet<OperatorInstanceId>();
        started = false;
        honMap = new HashMap<OperatorInstanceId, OperatorRunnable>();
        counterMap = new HashMap<String, Counter>();
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new ManagedWorkspaceFileFactory(this, (IOManager) joblet.getIOManager());
    }

    public void setOperator(OperatorDescriptorId odId, int partition, OperatorRunnable hon) {
        honMap.put(new OperatorInstanceId(odId, partition), hon);
    }

    public Map<OperatorInstanceId, OperatorRunnable> getOperatorMap() {
        return honMap;
    }

    public void setEndpointList(List<Endpoint> endpointList) {
        this.endpointList = endpointList;
    }

    public List<Endpoint> getEndpointList() {
        return endpointList;
    }

    public synchronized void start() throws Exception {
        if (started) {
            throw new Exception("Joblet already started");
        }
        started = true;
        notifyAll();
    }

    public synchronized void abort() {
        this.abort = true;
        for (OperatorRunnable r : honMap.values()) {
            r.abort();
        }
        notifyAll();
    }
    
    private synchronized boolean aborted() {
        return abort;
    }

    public void installRunnable(final OperatorInstanceId opIId) {
        pendingOperators.add(opIId);
        final OperatorRunnable hon = honMap.get(opIId);
        joblet.incrementOperatorCount();
        joblet.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        waitUntilStarted();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (aborted()) {
                        return;
                    }
                    try {
                        LOGGER.log(Level.INFO, joblet.getJobId() + ":" + stageId + ":" + opIId.getOperatorId() + ":"
                                + opIId.getPartition() + "(" + hon + ")" + ": STARTED");
                        hon.run();
                        LOGGER.log(Level.INFO, joblet.getJobId() + ":" + stageId + ":" + opIId.getOperatorId() + ":"
                                + opIId.getPartition() + "(" + hon + ")" + ": FINISHED");
                        notifyOperatorCompletion(opIId);
                    } catch (Exception e) {
                        LOGGER.log(Level.INFO, joblet.getJobId() + ":" + stageId + ":" + opIId.getOperatorId() + ":"
                                + opIId.getPartition() + "(" + hon + ")" + ": ABORTED");
                        e.printStackTrace();
                        notifyOperatorFailure(opIId);
                    }
                } finally {
                    joblet.decrementOperatorCount();
                }
            }
        });
    }

    protected void notifyOperatorCompletion(OperatorInstanceId opIId) {
        boolean done = false;
        synchronized (pendingOperators) {
            pendingOperators.remove(opIId);
            done = pendingOperators.isEmpty();
        }
        if (done) {
            try {
                StageletProfile sProfile = new StageletProfile(stageId);
                dumpProfile(sProfile);
                close();
                joblet.notifyStageletComplete(stageId, attempt, sProfile);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected synchronized void notifyOperatorFailure(OperatorInstanceId opIId) {
        abort();
        try {
            joblet.notifyStageletFailed(stageId, attempt);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized void waitUntilStarted() throws InterruptedException {
        while (!started && !abort) {
            wait();
        }
    }

    public synchronized void dumpProfile(StageletProfile sProfile) {
        Map<String, Long> dumpMap = sProfile.getCounters();
        for (Counter c : counterMap.values()) {
            dumpMap.put(c.getName(), c.get());
        }
    }

    @Override
    public IHyracksJobletContext getJobletContext() {
        return joblet;
    }

    @Override
    public UUID getStageId() {
        return stageId;
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
        return joblet.allocateFrame();
    }

    @Override
    public int getFrameSize() {
        return joblet.getFrameSize();
    }

    @Override
    public IIOManager getIOManager() {
        return joblet.getIOManager();
    }

    @Override
    public FileReference createWorkspaceFile(String prefix) throws HyracksDataException {
        return fileFactory.createWorkspaceFile(prefix);
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