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
package org.apache.hyracks.control.nc.application;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.application.IStateDumpHandler;
import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.api.resources.memory.IMemoryManager;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.common.application.ServiceContext;
import org.apache.hyracks.control.common.context.ServerContext;
import org.apache.hyracks.control.common.utils.HyracksThreadFactory;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.control.nc.resources.memory.MemoryManager;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.TraceCategoryRegistry;
import org.apache.hyracks.util.trace.Tracer;

public class NCServiceContext extends ServiceContext implements INCServiceContext {
    private final ILifeCycleComponentManager lccm;
    private final String nodeId;
    private final IOManager ioManager;
    private final MemoryManager memoryManager;
    private IStateDumpHandler sdh;
    private final NodeControllerService ncs;
    private IChannelInterfaceFactory messagingChannelInterfaceFactory;
    private final ITracer tracer;

    public NCServiceContext(NodeControllerService ncs, ServerContext serverCtx, IOManager ioManager, String nodeId,
            MemoryManager memoryManager, ILifeCycleComponentManager lifeCyclecomponentManager,
            IApplicationConfig appConfig) {
        super(serverCtx, appConfig, new HyracksThreadFactory(nodeId));
        this.lccm = lifeCyclecomponentManager;
        this.nodeId = nodeId;
        this.ioManager = ioManager;
        this.memoryManager = memoryManager;
        this.ncs = ncs;
        this.sdh = lccm::dumpState;
        this.tracer = new Tracer(nodeId, ncs.getConfiguration().getTraceCategories(), new TraceCategoryRegistry());
        this.distributedState = new ConcurrentHashMap<>();
    }

    @Override
    public ILifeCycleComponentManager getLifeCycleComponentManager() {
        return lccm;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    public void setDistributedState(Serializable state) {
        distributedState = state;
    }

    @Override
    public void setStateDumpHandler(IStateDumpHandler handler) {
        this.sdh = handler;
    }

    public IStateDumpHandler getStateDumpHandler() {
        return sdh;
    }

    @Override
    public IOManager getIoManager() {
        return ioManager;
    }

    @Override
    public IMemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public ITracer getTracer() {
        return tracer;
    }

    @Override
    public IControllerService getControllerService() {
        return ncs;
    }

    @Override
    public IChannelInterfaceFactory getMessagingChannelInterfaceFactory() {
        return messagingChannelInterfaceFactory;
    }

    @Override
    public void setMessagingChannelInterfaceFactory(IChannelInterfaceFactory interfaceFactory) {
        this.messagingChannelInterfaceFactory = interfaceFactory;
    }

    @Override
    public Object getApplicationContext() {
        return ncs.getApplicationContext();
    }
}
