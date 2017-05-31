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
package org.apache.asterix.runtime.utils;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.cluster.INodeManager;

public class RuntimeUtils {

    private RuntimeUtils() {
    }

    public static Set<String> getNodeControllersOnIP(ICcApplicationContext appCtx, InetAddress ipAddress)
            throws HyracksDataException {
        Map<InetAddress, Set<String>> nodeControllerInfo = getNodeControllerMap(appCtx);
        return nodeControllerInfo.get(ipAddress);
    }

    public static List<String> getAllNodeControllers(ICcApplicationContext appCtx) throws HyracksDataException {
        Collection<Set<String>> nodeControllersCollection = getNodeControllerMap(appCtx).values();
        List<String> nodeControllers = new ArrayList<>();
        for (Set<String> ncCollection : nodeControllersCollection) {
            nodeControllers.addAll(ncCollection);
        }
        return nodeControllers;
    }

    public static Map<InetAddress, Set<String>> getNodeControllerMap(ICcApplicationContext appCtx)
            throws HyracksDataException {
        Map<InetAddress, Set<String>> map = new HashMap<>();
        appCtx.getServiceContext().getCCContext().getIPAddressNodeMap(map);
        return map;
    }

    public static Map<InetAddress, Set<String>> getForcedNodeControllerMap(ICcApplicationContext appCtx) {
        ClusterControllerService ccs = (ClusterControllerService) appCtx.getServiceContext().getControllerService();
        INodeManager nodeManager = ccs.getNodeManager();
        return nodeManager.getIpAddressNodeNameMap();
    }

    public static JobSpecification createJobSpecification(ICcApplicationContext appCtx) {
        CompilerProperties compilerProperties = appCtx.getCompilerProperties();
        int frameSize = compilerProperties.getFrameSize();
        return new JobSpecification(frameSize);
    }
}
