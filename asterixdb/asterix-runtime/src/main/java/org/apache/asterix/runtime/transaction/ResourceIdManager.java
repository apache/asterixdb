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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.transactions.IResourceIdManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ResourceIdManager implements IResourceIdManager {

    private final IClusterStateManager csm;
    private final AtomicLong globalResourceId = new AtomicLong();
    private volatile Set<String> reportedNodes = new HashSet<>();
    private volatile boolean allReported = false;

    public ResourceIdManager(IClusterStateManager csm) {
        this.csm = csm;
    }

    @Override
    public long createResourceId() {
        if (!allReported) {
            synchronized (this) {
                if (!allReported) {
                    if (reportedNodes.size() < csm.getNumberOfNodes()) {
                        return -1;
                    } else {
                        reportedNodes = null;
                        allReported = true;
                    }
                }
            }
        }
        return globalResourceId.incrementAndGet();
    }

    @Override
    public synchronized boolean reported(String nodeId) {
        return allReported || reportedNodes.contains(nodeId);
    }

    @Override
    public synchronized void report(String nodeId, long maxResourceId) throws HyracksDataException {
        if (!allReported) {
            globalResourceId.set(Math.max(maxResourceId, globalResourceId.get()));
            reportedNodes.add(nodeId);
            if (reportedNodes.size() == csm.getNumberOfNodes()) {
                reportedNodes = null;
                allReported = true;
                csm.refreshState();
            }
        }
    }

}
