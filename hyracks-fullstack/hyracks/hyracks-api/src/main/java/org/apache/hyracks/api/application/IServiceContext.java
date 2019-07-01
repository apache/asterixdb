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
package org.apache.hyracks.api.application;

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.api.job.IJobSerializerDeserializerContainer;
import org.apache.hyracks.api.messages.IMessageBroker;
import org.apache.hyracks.api.service.IControllerService;

/**
 * Base interface of the {@link ICCServiceContext} and the {@link INCServiceContext}.
 */
public interface IServiceContext {
    /**
     * @return the distributed state that is made available to all the Application
     *         Contexts of this application in the cluster.
     */
    Serializable getDistributedState();

    void setMessageBroker(IMessageBroker messageBroker);

    IMessageBroker getMessageBroker();

    IJobSerializerDeserializerContainer getJobSerializerDeserializerContainer();

    ThreadFactory getThreadFactory();

    void setThreadFactory(ThreadFactory threadFactory);

    IApplicationConfig getAppConfig();

    /**
     * @return The controller service which the application context belongs to.
     */
    IControllerService getControllerService();

    Object getApplicationContext();

    /**
     * Sets the IPersistedResourceRegistry that contains the mapping between classes and type ids used
     * for serialization.
     *
     * @param persistedResourceRegistry
     */
    default void setPersistedResourceRegistry(IPersistedResourceRegistry persistedResourceRegistry) {
        throw new UnsupportedOperationException();
    }

    default IPersistedResourceRegistry getPersistedResourceRegistry() {
        throw new UnsupportedOperationException();
    }

    IServerContext getServerCtx();
}
