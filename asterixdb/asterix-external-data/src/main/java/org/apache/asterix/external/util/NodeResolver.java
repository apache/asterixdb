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
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.INodeResolver;

/**
 * Resolves a value (DNS/IP Address) or a (Node Controller Id) to the id of a Node Controller running at the location.
 */
public class NodeResolver implements INodeResolver {
    //TODO: change this call and replace by calling AsterixClusterProperties
    private final Random random = new Random();

    @Override
    public String resolveNode(ICcApplicationContext appCtx, String value, Map<InetAddress, Set<String>> ncMap,
            Set<String> ncs) throws AsterixException {
        if (ncs.contains(value)) {
            return value;
        }
        InetAddress ipAddress = null;
        try {
            ipAddress = InetAddress.getByName(value);
        } catch (UnknownHostException e) {
            throw new AsterixException(ErrorCode.NODE_RESOLVER_UNABLE_RESOLVE_HOST, e, value);
        }
        Set<String> nodeControllers = ncMap.get(ipAddress);
        if (nodeControllers == null || nodeControllers.isEmpty()) {
            throw new AsterixException(ErrorCode.NODE_RESOLVER_NO_NODE_CONTROLLERS, value);
        }
        return nodeControllers.toArray(new String[] {})[random.nextInt(nodeControllers.size())];
    }
}
