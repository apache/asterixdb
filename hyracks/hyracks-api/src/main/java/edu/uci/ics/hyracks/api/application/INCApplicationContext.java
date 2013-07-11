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
package edu.uci.ics.hyracks.api.application;

import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.resources.memory.IMemoryManager;

/**
 * Application Context at the Node Controller for an application.
 * 
 * @author vinayakb
 */
public interface INCApplicationContext extends IApplicationContext {
    /**
     * Gets the node Id of the Node Congtroller.
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
}