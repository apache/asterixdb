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
package org.apache.asterix.common.config;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.event.schema.cluster.Cluster;

public class ClusterProperties {

    public static final ClusterProperties INSTANCE = new ClusterProperties();

    private static final String CLUSTER_CONFIGURATION_FILE = "cluster.xml";
    private static final String DEFAULT_STORAGE_DIR_NAME = "storage";

    private final Cluster cluster;

    private ClusterProperties() {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(CLUSTER_CONFIGURATION_FILE);
        if (is != null) {
            try {
                JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
                Unmarshaller unmarshaller = ctx.createUnmarshaller();
                cluster = (Cluster) unmarshaller.unmarshal(is);
            } catch (JAXBException e) {
                throw new IllegalStateException("Failed to read configuration file " + CLUSTER_CONFIGURATION_FILE, e);
            }
        } else {
            cluster = null;
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public String getStorageDirectoryName() {
        if (cluster != null) {
            return cluster.getStore();
        }
        // virtual cluster without cluster config file
        return DEFAULT_STORAGE_DIR_NAME;
    }

    public boolean isReplicationEnabled() {
        if (cluster != null && cluster.getDataReplication() != null) {
            return cluster.getDataReplication().isEnabled();
        }
        return false;
    }

    public boolean isAutoFailoverEnabled() {
        return isReplicationEnabled() && cluster.getDataReplication().isAutoFailover();
    }
}