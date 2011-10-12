/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.io.Serializable;

/**
 * Base class of the {@link ICCApplicationContext} and the
 * {@link INCApplicationContext}.
 * 
 * @author vinayakb
 * 
 */
public interface IApplicationContext {
    /**
     * Provides the Class Loader that loads classes for this Hyracks Application
     * at the CC.
     * 
     * @return the application {@link ClassLoader}.
     */
    public ClassLoader getClassLoader();

    /**
     * Gets the distributed state that is made available to all the Application
     * Contexts of this application in the cluster.
     * 
     * @return
     */
    public Serializable getDistributedState();
}