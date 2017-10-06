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

import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.api.resources.memory.IMemoryManager;
import org.apache.hyracks.util.trace.ITracer;

/**
 * Service Context at the Node Controller for an application.
 */
public interface INCServiceContext extends IServiceContext {
    /**
     * Gets the life cycle component manager of the Node Controller.
     *
     * @return
     */
    ILifeCycleComponentManager getLifeCycleComponentManager();

    /**
     * Gets the node Id of the Node Controller.
     *
     * @return the Node Id.
     */
    String getNodeId();

    /**
     * @return the IO Manager
     */
    IIOManager getIoManager();

    /**
     * Get the memory manager at the node.
     *
     * @return Memory Manager
     */
    IMemoryManager getMemoryManager();

    /**
     * Get a Tracer to write trace events to.
     *
     * @return a Tracer
     */
    ITracer getTracer();

    /**
     * Set the handler for state dumps.
     *
     * @param handler
     */
    void setStateDumpHandler(IStateDumpHandler handler);

    /**
     * Set the application MessagingChannelInterfaceFactory
     *
     * @param interfaceFactory
     */
    void setMessagingChannelInterfaceFactory(IChannelInterfaceFactory interfaceFactory);

    /**
     * Get the application MessagingChannelInterfaceFactory previously set by
     * the {@link #setMessagingChannelInterfaceFactory(IChannelInterfaceFactory)} call.
     *
     * @return
     */
    IChannelInterfaceFactory getMessagingChannelInterfaceFactory();
}
