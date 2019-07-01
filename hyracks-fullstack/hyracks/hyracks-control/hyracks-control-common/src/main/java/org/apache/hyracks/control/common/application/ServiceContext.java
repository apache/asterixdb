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
package org.apache.hyracks.control.common.application;

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.api.application.IServerContext;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.api.job.IJobSerializerDeserializerContainer;
import org.apache.hyracks.api.job.JobSerializerDeserializerContainer;
import org.apache.hyracks.api.messages.IMessageBroker;

public abstract class ServiceContext implements IServiceContext {
    protected final IServerContext serverCtx;
    protected final IApplicationConfig appConfig;
    protected ThreadFactory threadFactory;
    protected Serializable distributedState;
    protected IMessageBroker messageBroker;
    protected IJobSerializerDeserializerContainer jobSerDeContainer = new JobSerializerDeserializerContainer();
    protected IPersistedResourceRegistry persistedResourceRegistry;

    public ServiceContext(IServerContext serverCtx, IApplicationConfig appConfig, ThreadFactory threadFactory) {
        this.serverCtx = serverCtx;
        this.appConfig = appConfig;
        this.threadFactory = threadFactory;
    }

    @Override
    public Serializable getDistributedState() {
        return distributedState;
    }

    @Override
    public void setMessageBroker(IMessageBroker messageBroker) {
        this.messageBroker = messageBroker;
    }

    @Override
    public IMessageBroker getMessageBroker() {
        return this.messageBroker;
    }

    @Override
    public IJobSerializerDeserializerContainer getJobSerializerDeserializerContainer() {
        return this.jobSerDeContainer;
    }

    @Override
    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    @Override
    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }

    @Override
    public IApplicationConfig getAppConfig() {
        return appConfig;
    }

    @Override
    public void setPersistedResourceRegistry(IPersistedResourceRegistry persistedResourceRegistry) {
        this.persistedResourceRegistry = persistedResourceRegistry;
    }

    @Override
    public IPersistedResourceRegistry getPersistedResourceRegistry() {
        return persistedResourceRegistry;
    }

    @Override
    public IServerContext getServerCtx() {
        return serverCtx;
    }
}
