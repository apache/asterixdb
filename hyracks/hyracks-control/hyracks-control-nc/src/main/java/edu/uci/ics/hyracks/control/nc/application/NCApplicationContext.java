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
package edu.uci.ics.hyracks.control.nc.application;

import java.io.IOException;
import java.io.Serializable;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.resources.memory.IMemoryManager;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.context.ServerContext;
import edu.uci.ics.hyracks.control.nc.resources.memory.MemoryManager;

public class NCApplicationContext extends ApplicationContext implements INCApplicationContext {
    private final String nodeId;
    private final IHyracksRootContext rootCtx;
    private final MemoryManager memoryManager;
    private Object appObject;

    public NCApplicationContext(ServerContext serverCtx, IHyracksRootContext rootCtx, String nodeId,
            MemoryManager memoryManager) throws IOException {
        super(serverCtx);
        this.nodeId = nodeId;
        this.rootCtx = rootCtx;
        this.memoryManager = memoryManager;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    public void setDistributedState(Serializable state) {
        distributedState = state;
    }

    @Override
    public IHyracksRootContext getRootContext() {
        return rootCtx;
    }

    @Override
    public void setApplicationObject(Object object) {
        this.appObject = object;
    }

    @Override
    public Object getApplicationObject() {
        return appObject;
    }

    @Override
    public IMemoryManager getMemoryManager() {
        return memoryManager;
    }
}