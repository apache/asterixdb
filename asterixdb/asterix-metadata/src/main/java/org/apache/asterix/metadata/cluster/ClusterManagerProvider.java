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
package org.apache.asterix.metadata.cluster;

import java.util.Collections;
import java.util.Set;

import org.apache.asterix.common.api.IClusterEventsSubscriber;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.metadata.api.IClusterManager;

public class ClusterManagerProvider {

    private ClusterManagerProvider() {
    }

    public static IClusterManager getClusterManager() {
        return Holder.INSTANCE;
    }

    private static final class Holder {
        static final IClusterManager INSTANCE;

        static {
            Cluster asterixCluster = ClusterProperties.INSTANCE.getCluster();
            String eventHome = asterixCluster == null ? null
                    : asterixCluster.getWorkingDir() == null ? null : asterixCluster.getWorkingDir().getDir();

            if (eventHome != null) {
                INSTANCE = new ClusterManager(eventHome);
            } else {
                INSTANCE = new NoopClusterManager();
            }
        }

        private Holder() {
        }
    }
    private static class NoopClusterManager implements IClusterManager {
        @Override
        public void addNode(Node node) throws AsterixException {
            // no-op
        }

        @Override
        public void removeNode(Node node) throws AsterixException {
            // no-op
        }

        @Override
        public void registerSubscriber(IClusterEventsSubscriber subscriber) {
            // no-op
        }

        @Override
        public boolean deregisterSubscriber(IClusterEventsSubscriber sunscriber) {
            return true;
        }

        @Override
        public Set<IClusterEventsSubscriber> getRegisteredClusterEventSubscribers() {
            return Collections.emptySet();
        }

        @Override
        public void notifyStartupCompleted() throws Exception {
            // no-op
        }
    }
}
