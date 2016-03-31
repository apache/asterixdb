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
package org.apache.asterix.installer.driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.common.configuration.AsterixConfiguration;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;

public class InstallerUtil {

    private static final String DEFAULT_ASTERIX_CONFIGURATION_PATH = "conf" + File.separator
            + "asterix-configuration.xml";

    public static final String TXN_LOG_DIR = "txnLogs";
    public static final String TXN_LOG_DIR_KEY_SUFFIX = "txnLogDir";
    public static final String ASTERIX_CONFIGURATION_FILE = "asterix-configuration.xml";
    public static final String TXN_LOG_CONFIGURATION_FILE = "log.properties";
    public static final int CLUSTER_NET_PORT_DEFAULT = 1098;
    public static final int CLIENT_NET_PORT_DEFAULT = 1099;
    public static final int HTTP_PORT_DEFAULT = 8888;
    public static final int WEB_INTERFACE_PORT_DEFAULT = 19001;

    public static String getNodeDirectories(String asterixInstanceName, Node node, Cluster cluster) {
        String storeDataSubDir = asterixInstanceName + File.separator + "data" + File.separator;
        String[] storeDirs = null;
        StringBuffer nodeDataStore = new StringBuffer();
        String storeDirValue = cluster.getStore();
        if (storeDirValue == null) {
            throw new IllegalStateException(" Store not defined for node " + node.getId());
        }
        storeDataSubDir = node.getId() + File.separator + storeDataSubDir;

        storeDirs = storeDirValue.split(",");
        for (String ns : storeDirs) {
            nodeDataStore.append(ns + File.separator + storeDataSubDir.trim());
            nodeDataStore.append(",");
        }
        nodeDataStore.deleteCharAt(nodeDataStore.length() - 1);
        return nodeDataStore.toString();
    }

    public static AsterixConfiguration getAsterixConfiguration(String asterixConf)
            throws FileNotFoundException, IOException, JAXBException {
        if (asterixConf == null) {
            asterixConf = InstallerDriver.getManagixHome() + File.separator + DEFAULT_ASTERIX_CONFIGURATION_PATH;
        }
        File file = new File(asterixConf);
        JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        AsterixConfiguration asterixConfiguration = (AsterixConfiguration) unmarshaller.unmarshal(file);
        return asterixConfiguration;
    }
}
