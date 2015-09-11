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

import org.apache.hyracks.api.context.IHyracksRootContext;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.api.resources.memory.IMemoryManager;

/**
 * Application Context at the Node Controller for an application.
 * 
 * @author vinayakb
 */
public interface INCApplicationContext extends IApplicationContext {
    /**
     * Gets the life cycle component manager of the Node Controller.
     * 
     * @return
     */
    public ILifeCycleComponentManager getLifeCycleComponentManager();

    /**
     * Gets the node Id of the Node Controller.
     * 
     * @return the Node Id.
     */
    public String getNodeId();

    /**
     * Get the Hyracks Root Context.
     * 
     * @return The Hyracks Root Context
     */
    public IHyracksRootContext getRootContext();

    /**
     * Set an object that can be later retrieved by the {@link #getApplicationObject()} call.
     * 
     * @param object
     *            Application Object
     */
    public void setApplicationObject(Object object);

    /**
     * Get the application object previously set by the {@link #setApplicationObject(Object)} call.
     * 
     * @return Application Object
     */
    public Object getApplicationObject();

    /**
     * Get the memory manager at the node.
     * 
     * @return Memory Manager
     */
    public IMemoryManager getMemoryManager();

    /**
     * Set the handler for state dumps.
     * 
     * @param handler
     */
    public void setStateDumpHandler(IStateDumpHandler handler);
}