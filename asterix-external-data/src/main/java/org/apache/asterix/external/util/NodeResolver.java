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
package org.apache.asterix.external.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.INodeResolver;
import org.apache.asterix.om.util.AsterixRuntimeUtil;

/**
 * Resolves a value (DNS/IP Address) or a (Node Controller Id) to the id of a Node Controller running at the location.
 */
public class NodeResolver implements INodeResolver {
    //TODO: change this call and replace by calling AsterixClusterProperties
    private static final Random random = new Random();
    private static final Map<InetAddress, Set<String>> ncMap = new HashMap<InetAddress, Set<String>>();
    private static final Set<String> ncs = new HashSet<String>();

    @Override
    public String resolveNode(String value) throws AsterixException {
        UnknownHostException uhe = null;
        try {
            if (ncMap.isEmpty()) {
                NodeResolver.updateNCs();
            }
            InetAddress ipAddress = null;
            try {
                ipAddress = InetAddress.getByName(value);
            } catch (UnknownHostException e) {
                uhe = e;
            }
            if (ipAddress == null) {
                if (ncs.contains(value)) {
                    return value;
                } else {
                    NodeResolver.updateNCs();
                    if (ncs.contains(value)) {
                        return value;
                    } else {
                        throw new AsterixException("address passed: '" + value
                                + "' couldn't be resolved to an ip address and is not an NC id. Existing NCs are "
                                + ncs.toString(), uhe);
                    }
                }

            }
            Set<String> nodeControllers = ncMap.get(ipAddress);
            if (nodeControllers == null || nodeControllers.isEmpty()) {
                throw new AsterixException(" No node controllers found at the address: " + value);
            }
            String chosenNCId = nodeControllers.toArray(new String[] {})[random.nextInt(nodeControllers.size())];
            return chosenNCId;
        } catch (UnknownHostException e) {
            throw new AsterixException("Unable to resolve hostname '" + value + "' to an IP address");
        } catch (AsterixException ae) {
            throw ae;
        } catch (Exception e) {
            throw new AsterixException(e);
        }
    }

    private static void updateNCs() throws Exception {
        synchronized (ncMap) {
            ncMap.clear();
            AsterixRuntimeUtil.getNodeControllerMap(ncMap);
            synchronized (ncs) {
                ncs.clear();
                for (Entry<InetAddress, Set<String>> entry : ncMap.entrySet()) {
                    ncs.addAll(entry.getValue());
                }
            }
        }
    }
}
