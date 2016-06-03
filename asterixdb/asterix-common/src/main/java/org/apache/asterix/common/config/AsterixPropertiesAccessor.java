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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.configuration.AsterixConfiguration;
import org.apache.asterix.common.configuration.Coredump;
import org.apache.asterix.common.configuration.Property;
import org.apache.asterix.common.configuration.Store;
import org.apache.asterix.common.configuration.TransactionLogDir;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.hyracks.api.application.IApplicationConfig;

public class AsterixPropertiesAccessor {
    private static Logger LOGGER = Logger.getLogger(AsterixPropertiesAccessor.class.getName());

    private final String instanceName;
    private final String metadataNodeName;
    private final List<String> nodeNames = new ArrayList<>();;
    private final Map<String, String[]> stores = new HashMap<>();;
    private final Map<String, String> coredumpConfig = new HashMap<>();
    private final Map<String, Property> asterixConfigurationParams;
    private final IApplicationConfig cfg;
    private final Map<String, String> transactionLogDirs = new HashMap<>();
    private final Map<String, String> asterixBuildProperties;
    private final Map<String, ClusterPartition[]> nodePartitionsMap;
    private final SortedMap<Integer, ClusterPartition> clusterPartitions = new TreeMap<>();

    /**
     * Constructor which reads asterix-configuration.xml, the old way.
     * @throws AsterixException
     */
    public AsterixPropertiesAccessor() throws AsterixException {
        String fileName = System.getProperty(GlobalConfig.CONFIG_FILE_PROPERTY);
        if (fileName == null) {
            fileName = GlobalConfig.DEFAULT_CONFIG_FILE_NAME;
        }

        InputStream is = this.getClass().getClassLoader().getResourceAsStream(fileName);
        if (is == null) {
            try {
                fileName = GlobalConfig.DEFAULT_CONFIG_FILE_NAME;
                is = new FileInputStream(fileName);
            } catch (FileNotFoundException fnf) {
                throw new AsterixException("Could not find configuration file " + fileName);
            }
        }

        AsterixConfiguration asterixConfiguration = null;
        cfg = null;
        try {
            JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
            Unmarshaller unmarshaller = ctx.createUnmarshaller();
            asterixConfiguration = (AsterixConfiguration) unmarshaller.unmarshal(is);
        } catch (JAXBException e) {
            throw new AsterixException("Failed to read configuration file " + fileName);
        }
        instanceName = asterixConfiguration.getInstanceName();
        metadataNodeName = asterixConfiguration.getMetadataNode();
        List<Store> configuredStores = asterixConfiguration.getStore();
        nodePartitionsMap = new HashMap<>();
        int uniquePartitionId = 0;
        for (Store store : configuredStores) {
            String trimmedStoreDirs = store.getStoreDirs().trim();
            String[] nodeStores = trimmedStoreDirs.split(",");
            ClusterPartition[] nodePartitions = new ClusterPartition[nodeStores.length];
            for (int i = 0; i < nodePartitions.length; i++) {
                ClusterPartition partition = new ClusterPartition(uniquePartitionId++, store.getNcId(), i);
                clusterPartitions.put(partition.getPartitionId(), partition);
                nodePartitions[i] = partition;
            }
            stores.put(store.getNcId(), nodeStores);
            nodePartitionsMap.put(store.getNcId(), nodePartitions);
            nodeNames.add(store.getNcId());
        }
        asterixConfigurationParams = new HashMap<String, Property>();
        for (Property p : asterixConfiguration.getProperty()) {
            asterixConfigurationParams.put(p.getName(), p);
        }
        for (Coredump cd : asterixConfiguration.getCoredump()) {
            coredumpConfig.put(cd.getNcId(), cd.getCoredumpPath());
        }
        for (TransactionLogDir txnLogDir : asterixConfiguration.getTransactionLogDir()) {
            transactionLogDirs.put(txnLogDir.getNcId(), txnLogDir.getTxnLogDirPath());
        }
        Properties gitProperties = new Properties();
        try {
            gitProperties.load(getClass().getClassLoader().getResourceAsStream("git.properties"));
            asterixBuildProperties = new HashMap<String, String>();
            for (final String name : gitProperties.stringPropertyNames()) {
                asterixBuildProperties.put(name, gitProperties.getProperty(name));
            }
        } catch (IOException e) {
            throw new AsterixException(e);
        }
    }

    /**
     * Constructor which wraps an IApplicationConfig.
     */
    public AsterixPropertiesAccessor(IApplicationConfig cfg) {
        this.cfg = cfg;
        instanceName = cfg.getString("asterix", "instance", "DEFAULT_INSTANCE");
        String mdNode = null;
        nodePartitionsMap = new HashMap<>();
        int uniquePartitionId = 0;
        for (String section : cfg.getSections()) {
            if (!section.startsWith("nc/")) {
                continue;
            }
            String ncId = section.substring(3);

            if (mdNode == null) {
                // Default is first node == metadata node
                mdNode = ncId;
            }
            if (cfg.getString(section, "metadata.port") != null) {
                // QQQ But we don't actually *honor* metadata.port yet!
                mdNode = ncId;
            }

            // QQQ Default values? Should they be specified here? Or should there
            // be a default.ini? They can't be inserted by TriggerNCWork except
            // possibly for hyracks-specified values. Certainly wherever they are,
            // they should be platform-dependent.
            coredumpConfig.put(ncId, cfg.getString(section, "coredumpdir", "/var/lib/asterixdb/coredump"));
            transactionLogDirs.put(ncId, cfg.getString(section, "txnlogdir", "/var/lib/asterixdb/txn-log"));
            String[] storeDirs = cfg.getString(section, "storagedir", "storage").trim().split(",");
            ClusterPartition[] nodePartitions = new ClusterPartition[storeDirs.length];
            for (int i = 0; i < nodePartitions.length; i++) {
                ClusterPartition partition = new ClusterPartition(uniquePartitionId++, ncId, i);
                clusterPartitions.put(partition.getPartitionId(), partition);
                nodePartitions[i] = partition;
            }
            stores.put(ncId, storeDirs);
            nodePartitionsMap.put(ncId, nodePartitions);
            nodeNames.add(ncId);
        }

        metadataNodeName = mdNode;
        asterixConfigurationParams = null;
        asterixBuildProperties = null;
    }

    public String getMetadataNodeName() {
        return metadataNodeName;
    }

    public Map<String, String[]> getStores() {
        return stores;
    }

    public List<String> getNodeNames() {
        return nodeNames;
    }

    public String getCoredumpPath(String nodeId) {
        return coredumpConfig.get(nodeId);
    }

    public Map<String, String> getTransactionLogDirs() {
        return transactionLogDirs;
    }

    public Map<String, String> getCoredumpConfig() {
        return coredumpConfig;
    }

    public Map<String, String> getBuildProperties() {
        return asterixBuildProperties;
    }

    public void putCoredumpPaths(String nodeId, String coredumpPath) {
        if (coredumpConfig.containsKey(nodeId)) {
            throw new IllegalStateException("Cannot override value for coredump path");
        }
        coredumpConfig.put(nodeId, coredumpPath);
    }

    public void putTransactionLogDir(String nodeId, String txnLogDir) {
        if (transactionLogDirs.containsKey(nodeId)) {
            throw new IllegalStateException("Cannot override value for txnLogDir");
        }
        transactionLogDirs.put(nodeId, txnLogDir);
    }

    public <T> T getProperty(String property, T defaultValue, IPropertyInterpreter<T> interpreter) {
        String value;
        Property p = null;
        if (asterixConfigurationParams != null) {
            p = asterixConfigurationParams.get(property);
            value = (p == null) ? null : p.getValue();
        } else {
            value = cfg.getString("asterix", property);
        }
        if (value == null) {
            return defaultValue;
        }
        try {
            return interpreter.interpret(value);
        } catch (IllegalArgumentException e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                StringBuilder msg =
                        new StringBuilder("Invalid property value '" + value + "' for property '" + property + "'.\n");
                if (p != null) {
                    msg.append("See the description: \n" + p.getDescription() + "\n");
                }
                msg.append("Default = " + defaultValue);
                LOGGER.severe(msg.toString());
            }
            throw e;
        }
    }

    private static <T> void logConfigurationError(Property p, T defaultValue) {
        if (LOGGER.isLoggable(Level.SEVERE)) {
            LOGGER.severe("Invalid property value '" + p.getValue() + "' for property '" + p.getName()
                    + "'.\n See the description: \n" + p.getDescription() + "\nDefault = " + defaultValue);
        }
    }

    public String getInstanceName() {
        return instanceName;
    }

    public ClusterPartition getMetadataPartiton() {
        // metadata partition is always the first partition on the metadata node
        return nodePartitionsMap.get(metadataNodeName)[0];
    }

    public Map<String, ClusterPartition[]> getNodePartitions() {
        return nodePartitionsMap;
    }

    public SortedMap<Integer, ClusterPartition> getClusterPartitions() {
        return clusterPartitions;
    }
}
