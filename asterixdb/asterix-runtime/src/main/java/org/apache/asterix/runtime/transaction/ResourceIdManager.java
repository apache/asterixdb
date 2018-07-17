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
package org.apache.asterix.runtime.transaction;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.transactions.IResourceIdManager;

public class ResourceIdManager implements IResourceIdManager {

    private final IClusterStateManager csm;
    private final AtomicLong globalResourceId = new AtomicLong();
    private Set<String> reportedNodes = ConcurrentHashMap.newKeySet();

    public ResourceIdManager(IClusterStateManager csm) {
        this.csm = csm;
    }

    @Override
    public long createResourceId() {
        return csm.isClusterActive() || reportedNodes.containsAll(csm.getParticipantNodes(true))
                ? globalResourceId.incrementAndGet() : -1;
    }

    @Override
    public boolean reported(String nodeId) {
        return reportedNodes.contains(nodeId);
    }

    @Override
    public void report(String nodeId, long maxResourceId) {
        globalResourceId.updateAndGet(prev -> Math.max(maxResourceId, prev));
        reportedNodes.add(nodeId);
    }
}
