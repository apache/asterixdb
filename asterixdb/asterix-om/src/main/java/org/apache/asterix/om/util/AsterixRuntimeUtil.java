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
package org.apache.asterix.om.util;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for obtaining information on the set of Hyracks NodeController
 * processes that are running on a given host.
 */
public class AsterixRuntimeUtil {

    public static Set<String> getNodeControllersOnIP(InetAddress ipAddress) throws Exception {
        Map<InetAddress, Set<String>> nodeControllerInfo = getNodeControllerMap();
        Set<String> nodeControllersAtLocation = nodeControllerInfo.get(ipAddress);
        return nodeControllersAtLocation;
    }

    public static List<String> getAllNodeControllers() throws Exception {
        Collection<Set<String>> nodeControllersCollection = getNodeControllerMap().values();
        List<String> nodeControllers = new ArrayList<String>();
        for (Set<String> ncCollection : nodeControllersCollection) {
            nodeControllers.addAll(ncCollection);
        }
        return nodeControllers;
    }

    public static Map<InetAddress, Set<String>> getNodeControllerMap() throws Exception {
        Map<InetAddress, Set<String>> map = new HashMap<InetAddress, Set<String>>();
        AsterixAppContextInfo.getInstance().getCCApplicationContext().getCCContext().getIPAddressNodeMap(map);
        return map;
    }

    public static void getNodeControllerMap(Map<InetAddress, Set<String>> map) throws Exception {
        AsterixAppContextInfo.getInstance().getCCApplicationContext().getCCContext().getIPAddressNodeMap(map);
    }
}
