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
package org.apache.asterix.app.cc;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.transactions.IAsterixResourceIdManager;
import org.apache.asterix.runtime.util.AsterixClusterProperties;

public class AsterixResourceIdManager implements IAsterixResourceIdManager {

    private final AtomicLong globalResourceId = new AtomicLong();
    private volatile Set<String> reportedNodes = new HashSet<>();
    private volatile boolean allReported = false;

    @Override
    public long createResourceId() {
        if (!allReported) {
            synchronized (this) {
                if (!allReported) {
                    if (reportedNodes.size() < AsterixClusterProperties.getNumberOfNodes()) {
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
    public synchronized void report(String nodeId, long maxResourceId) {
        if (!allReported) {
            globalResourceId.set(Math.max(maxResourceId, globalResourceId.get()));
            reportedNodes.add(nodeId);
            if (reportedNodes.size() == AsterixClusterProperties.getNumberOfNodes()) {
                reportedNodes = null;
                allReported = true;
            }
        }
    }

}
