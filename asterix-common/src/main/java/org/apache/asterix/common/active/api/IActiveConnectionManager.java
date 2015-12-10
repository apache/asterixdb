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
package org.apache.asterix.common.active.api;

import java.io.IOException;
import java.util.List;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.active.ActiveRuntime;
import org.apache.asterix.common.active.ActiveRuntimeId;
import org.apache.asterix.common.active.ActiveRuntimeManager;

/**
 * Handle (de)registration of feeds for delivery of control messages.
 */
public interface IActiveConnectionManager {

    /**
     * Allows registration of an activeRuntime.
     * 
     * @param activeRuntime
     * @param activeId
     * @throws Exception
     */
    public void registerActiveRuntime(ActiveJobId activeJobId, ActiveRuntime activeRuntime) throws Exception;

    /**
     * Obtain active runtime corresponding to a activeRuntimeId
     * 
     * @param activeRuntimeId
     * @param activeId
     * @return
     */
    public ActiveRuntime getActiveRuntime(ActiveJobId activeJobId, ActiveRuntimeId activeRuntimeId);

    /**
     * De-register an active object
     * 
     * @param activeId
     * @throws IOException
     */
    public void deregister(ActiveJobId activeJobId);

    /**
     * Obtain the active runtime manager associated with an active object.
     * 
     * @param activeId
     * @return
     */
    public ActiveRuntimeManager getActiveRuntimeManager(ActiveJobId activeJobId);

    /**
     * Allows de-registration of an active runtime.
     * 
     * @param activeRuntimeId
     */
    void deRegisterActiveRuntime(ActiveJobId activeJobId, ActiveRuntimeId activeRuntimeId);

    public List<ActiveRuntimeId> getRegisteredRuntimes();

}
