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
package edu.uci.ics.asterix.om.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
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

    public static Set<String> getNodeControllersOnIP(String ipAddress) throws Exception {
        Map<String, Set<String>> nodeControllerInfo = getNodeControllerMap();
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

    public static Map<String, Set<String>> getNodeControllerMap() throws Exception {
        Map<String, Set<String>> map = new HashMap<String, Set<String>>();
        AsterixAppContextInfo.getInstance().getCCApplicationContext().getCCContext().getIPAddressNodeMap(map);
        return map;
    }

    public static String getIPAddress(String hostname) throws UnknownHostException {
        String address = InetAddress.getByName(hostname).getHostAddress();
        if (address.equals("127.0.1.1")) {
            address = "127.0.0.1";
        }
        return address;
    }
}
