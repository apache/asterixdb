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
package edu.uci.ics.hyracks.control.cc.application;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.application.ICCBootstrap;
import edu.uci.ics.hyracks.api.context.ICCContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IJobLifecycleListener;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.context.ServerContext;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;

public class CCApplicationContext extends ApplicationContext implements ICCApplicationContext {
    private final ICCContext ccContext;

    protected final Set<String> initPendingNodeIds;
    protected final Set<String> deinitPendingNodeIds;

    protected IResultCallback<Object> initializationCallback;
    protected IResultCallback<Object> deinitializationCallback;

    private List<IJobLifecycleListener> jobLifecycleListeners;

    public CCApplicationContext(ServerContext serverCtx, ICCContext ccContext, String appName) throws IOException {
        super(serverCtx, appName);
        this.ccContext = ccContext;
        initPendingNodeIds = new HashSet<String>();
        deinitPendingNodeIds = new HashSet<String>();
        jobLifecycleListeners = new ArrayList<IJobLifecycleListener>();
    }

    @Override
    protected void start() throws Exception {
        ((ICCBootstrap) bootstrap).setApplicationContext(this);
        bootstrap.start();
    }

    public ICCContext getCCContext() {
        return ccContext;
    }

    public JobSpecification createJobSpecification(byte[] bytes) throws HyracksException {
        try {
            return (JobSpecification) JavaSerializationUtils.deserialize(bytes, getClassLoader());
        } catch (IOException e) {
            throw new HyracksException(e);
        } catch (ClassNotFoundException e) {
            throw new HyracksException(e);
        }
    }

    @Override
    protected void stop() throws Exception {
        if (bootstrap != null) {
            bootstrap.stop();
        }
    }

    @Override
    public void setDistributedState(Serializable state) {
        this.distributedState = state;
    }

    @Override
    public void addJobLifecycleListener(IJobLifecycleListener jobLifecycleListener) {
        jobLifecycleListeners.add(jobLifecycleListener);
    }

    public synchronized void notifyJobStart(JobId jobId) throws HyracksException {
        for (IJobLifecycleListener l : jobLifecycleListeners) {
            l.notifyJobStart(jobId);
        }
    }

    public synchronized void notifyJobFinish(JobId jobId) throws HyracksException {
        for (IJobLifecycleListener l : jobLifecycleListeners) {
            l.notifyJobFinish(jobId);
        }
    }

    public synchronized void notifyJobCreation(JobId jobId, IOperatorDescriptorRegistry specification)
            throws HyracksException {
        for (IJobLifecycleListener l : jobLifecycleListeners) {
            l.notifyJobCreation(jobId, specification);
        }
    }

    public Set<String> getInitializationPendingNodeIds() {
        return initPendingNodeIds;
    }

    public Set<String> getDeinitializationPendingNodeIds() {
        return deinitPendingNodeIds;
    }

    public IResultCallback<Object> getInitializationCallback() {
        return initializationCallback;
    }

    public void setInitializationCallback(IResultCallback<Object> initializationCallback) {
        this.initializationCallback = initializationCallback;
    }

    public IResultCallback<Object> getDeinitializationCallback() {
        return deinitializationCallback;
    }

    public void setDeinitializationCallback(IResultCallback<Object> deinitializationCallback) {
        this.deinitializationCallback = deinitializationCallback;
    }
}