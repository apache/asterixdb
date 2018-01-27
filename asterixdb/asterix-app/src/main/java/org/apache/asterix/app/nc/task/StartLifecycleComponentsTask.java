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
package org.apache.asterix.app.nc.task;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.hyracks.bootstrap.AsterixStateDumpHandler;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.api.lifecycle.LifeCycleComponentManager;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.nc.application.NCServiceContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StartLifecycleComponentsTask implements INCLifecycleTask {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    @Override
    public void perform(CcId ccId, IControllerService cs) throws HyracksDataException {
        INcApplicationContext applicationContext = (INcApplicationContext) cs.getApplicationContext();
        NCServiceContext serviceCtx = (NCServiceContext) cs.getContext();
        MetadataProperties metadataProperties = applicationContext.getMetadataProperties();
        LOGGER.info("Starting lifecycle components");
        Map<String, String> lifecycleMgmtConfiguration = new HashMap<>();
        String dumpPathKey = LifeCycleComponentManager.Config.DUMP_PATH_KEY;
        String dumpPath = metadataProperties.getCoredumpPath(serviceCtx.getNodeId());
        lifecycleMgmtConfiguration.put(dumpPathKey, dumpPath);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Coredump directory for NC is: " + dumpPath);
        }
        ILifeCycleComponentManager lccm = serviceCtx.getLifeCycleComponentManager();
        lccm.configure(lifecycleMgmtConfiguration);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Configured:" + lccm);
        }
        serviceCtx.setStateDumpHandler(new AsterixStateDumpHandler(serviceCtx.getNodeId(), lccm.getDumpPath(), lccm));
        lccm.startAll();
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\" }";
    }
}
