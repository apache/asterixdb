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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.application.IClusterLifecycleListener;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.context.ICCContext;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.application.ServiceContext;
import org.apache.hyracks.control.common.context.ServerContext;
import org.apache.hyracks.control.common.utils.HyracksThreadFactory;

public class CCServiceContext extends ServiceContext implements ICCServiceContext {
    private final ICCContext ccContext;

    protected final Set<String> initPendingNodeIds;
    protected final Set<String> deinitPendingNodeIds;

    private List<IJobLifecycleListener> jobLifecycleListeners;
    private List<IClusterLifecycleListener> clusterLifecycleListeners;
    private final ClusterControllerService ccs;

    public CCServiceContext(ClusterControllerService ccs, ServerContext serverCtx, ICCContext ccContext,
            IApplicationConfig appConfig) throws IOException {
        super(serverCtx, appConfig, new HyracksThreadFactory("ClusterController"));
        this.ccContext = ccContext;
        this.ccs = ccs;
        initPendingNodeIds = new HashSet<>();
        deinitPendingNodeIds = new HashSet<>();
        jobLifecycleListeners = new ArrayList<>();
        clusterLifecycleListeners = new ArrayList<>();
    }

    @Override
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

    public synchronized void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions)
            throws HyracksException {
        for (IJobLifecycleListener l : jobLifecycleListeners) {
            l.notifyJobFinish(jobId, jobStatus, exceptions);
        }
    }

    public synchronized void notifyJobCreation(JobId jobId, JobSpecification spec) throws HyracksException {
        for (IJobLifecycleListener l : jobLifecycleListeners) {
            l.notifyJobCreation(jobId, spec);
        }
    }

    @Override
    public void addClusterLifecycleListener(IClusterLifecycleListener clusterLifecycleListener) {
        clusterLifecycleListeners.add(clusterLifecycleListener);
    }

    public void notifyNodeJoin(String nodeId, Map<IOption, Object> ncConfiguration) throws HyracksException {
        for (IClusterLifecycleListener l : clusterLifecycleListeners) {
            l.notifyNodeJoin(nodeId, ncConfiguration);
        }
    }

    public void notifyNodeFailure(Collection<String> deadNodeIds) throws HyracksException {
        for (IClusterLifecycleListener l : clusterLifecycleListeners) {
            l.notifyNodeFailure(deadNodeIds);
        }
    }

    @Override
    public IControllerService getControllerService() {
        return ccs;
    }

    @Override
    public Object getApplicationContext() {
        return ccs.getApplicationContext();
    }
}
