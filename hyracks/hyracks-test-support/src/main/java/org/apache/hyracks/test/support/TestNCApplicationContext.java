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
package org.apache.hyracks.test.support;

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.application.IStateDumpHandler;
import org.apache.hyracks.api.context.IHyracksRootContext;
import org.apache.hyracks.api.job.IJobSerializerDeserializerContainer;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.api.lifecycle.LifeCycleComponentManager;
import org.apache.hyracks.api.messages.IMessageBroker;
import org.apache.hyracks.api.resources.memory.IMemoryManager;

public class TestNCApplicationContext implements INCApplicationContext {
    private final ILifeCycleComponentManager lccm;
    private final IHyracksRootContext rootCtx;
    private final String nodeId;

    private Serializable distributedState;
    private Object appObject;

    private final IMemoryManager mm;

    public TestNCApplicationContext(IHyracksRootContext rootCtx, String nodeId) {
        this.lccm = new LifeCycleComponentManager();
        this.rootCtx = rootCtx;
        this.nodeId = nodeId;
        mm = new IMemoryManager() {
            @Override
            public long getMaximumMemory() {
                return Long.MAX_VALUE;
            }

            @Override
            public long getAvailableMemory() {
                return Long.MAX_VALUE;
            }

            @Override
            public void deallocate(long memory) {

            }

            @Override
            public boolean allocate(long memory) {
                return true;
            }
        };
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public Serializable getDistributedState() {
        return distributedState;
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
    public void setMessageBroker(IMessageBroker staticticsConnector) {
    }

    @Override
    public IMessageBroker getMessageBroker() {
        return null;
    }

    @Override
    public IJobSerializerDeserializerContainer getJobSerializerDeserializerContainer() {
        return null;
    }

    @Override
    public IMemoryManager getMemoryManager() {
        return mm;
    }

    @Override
    public ThreadFactory getThreadFactory() {
        return null;
    }

    @Override
    public void setThreadFactory(ThreadFactory threadFactory) {
    }

    @Override
    public ILifeCycleComponentManager getLifeCycleComponentManager() {
        return lccm;
    }

    @Override
    public void setStateDumpHandler(IStateDumpHandler handler) {
    }
}
