/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.control.cc.application;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.application.IClusterLifecycleListener;
import org.apache.hyracks.api.context.ICCContext;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.application.ApplicationContext;
import org.apache.hyracks.control.common.context.ServerContext;
import org.apache.hyracks.control.common.work.IResultCallback;

public class CCApplicationContext extends ApplicationContext implements ICCApplicationContext {
    private final ICCContext ccContext;

    protected final Set<String> initPendingNodeIds;
    protected final Set<String> deinitPendingNodeIds;

    protected IResultCallback<Object> initializationCallback;
    protected IResultCallback<Object> deinitializationCallback;

    private List<IJobLifecycleListener> jobLifecycleListeners;
    private List<IClusterLifecycleListener> clusterLifecycleListeners;

    public CCApplicationContext(ServerContext serverCtx, ICCContext ccContext) throws IOException {
        super(serverCtx);
        this.ccContext = ccContext;
        initPendingNodeIds = new HashSet<String>();
        deinitPendingNodeIds = new HashSet<String>();
        jobLifecycleListeners = new ArrayList<IJobLifecycleListener>();
        clusterLifecycleListeners = new ArrayList<IClusterLifecycleListener>();
    }

    public ICCContext getCCContext() {
        return ccContext;
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

    public synchronized void notifyJobCreation(JobId jobId, IActivityClusterGraphGeneratorFactory acggf)
            throws HyracksException {
        for (IJobLifecycleListener l : jobLifecycleListeners) {
            l.notifyJobCreation(jobId, acggf);
        }
    }

    @Override
    public void addClusterLifecycleListener(IClusterLifecycleListener clusterLifecycleListener) {
        clusterLifecycleListeners.add(clusterLifecycleListener);
    }

    public void notifyNodeJoin(String nodeId, Map<String, String> ncConfiguration) throws HyracksException {
        for (IClusterLifecycleListener l : clusterLifecycleListeners) {
            l.notifyNodeJoin(nodeId, ncConfiguration);
        }
    }

    public void notifyNodeFailure(Set<String> deadNodeIds) {
        for (IClusterLifecycleListener l : clusterLifecycleListeners) {
            l.notifyNodeFailure(deadNodeIds);
        }
    }
}