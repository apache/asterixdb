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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.configuration.AsterixConfiguration;
import org.apache.asterix.common.configuration.Coredump;
import org.apache.asterix.common.configuration.Extension;
import org.apache.asterix.common.configuration.Property;
import org.apache.asterix.common.configuration.Store;
import org.apache.asterix.common.configuration.TransactionLogDir;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.utils.ConfigUtil;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.IApplicationConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class AsterixPropertiesAccessor {
    private static final Logger LOGGER = Logger.getLogger(AsterixPropertiesAccessor.class.getName());

    private final String instanceName;
    private final String metadataNodeName;
    private final List<String> nodeNames = new ArrayList<>();;
    private final Map<String, String[]> stores = new HashMap<>();;
    private final Map<String, String> coredumpConfig = new HashMap<>();

    // This can be removed when asterix-configuration.xml is no longer required.
    private final Map<String, Property> asterixConfigurationParams;
    private final IApplicationConfig cfg;
    private final Map<String, String> transactionLogDirs = new HashMap<>();
    private final Map<String, String> asterixBuildProperties = new HashMap<>();
    private final Map<String, ClusterPartition[]> nodePartitionsMap;
    private final SortedMap<Integer, ClusterPartition> clusterPartitions = new TreeMap<>();
    // For extensions
    private final List<AsterixExtension> extensions;

    /**
     * Constructor which reads asterix-configuration.xml, the old way.
     *
     * @throws AsterixException
     * @throws IOException
     */
    public AsterixPropertiesAccessor() throws AsterixException, IOException {
        String fileName = System.getProperty(GlobalConfig.CONFIG_FILE_PROPERTY);
        if (fileName == null) {
            fileName = GlobalConfig.DEFAULT_CONFIG_FILE_NAME;
        }
        AsterixConfiguration asterixConfiguration = configure(fileName);
        cfg = null;
        instanceName = asterixConfiguration.getInstanceName();
        metadataNodeName = asterixConfiguration.getMetadataNode();
        List<Store> configuredStores = asterixConfiguration.getStore();
        nodePartitionsMap = new HashMap<>();
        int uniquePartitionId = 0;
        // Here we iterate through all <store> elements in asterix-configuration.xml.
        // For each one, we create an array of ClusterPartitions and store this array
        // in nodePartitionsMap, keyed by the node name. The array is the same length
        // as the comma-separated <storeDirs> child element, because Managix will have
        // arranged for that element to be populated with the full paths to each
        // partition directory (as formed by appending the <store> subdirectory to
        // each <iodevices> path from the user's original cluster.xml).
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

        // Get extensions
        extensions = new ArrayList<>();
        if (asterixConfiguration.getExtensions() != null) {
            for (Extension ext : asterixConfiguration.getExtensions().getExtension()) {
                extensions.add(ConfigUtil.toAsterixExtension(ext));
            }
        }

        asterixConfigurationParams = new HashMap<>();
        for (Property p : asterixConfiguration.getProperty()) {
            asterixConfigurationParams.put(p.getName(), p);
        }
        for (Coredump cd : asterixConfiguration.getCoredump()) {
            coredumpConfig.put(cd.getNcId(), cd.getCoredumpPath());
        }
        for (TransactionLogDir txnLogDir : asterixConfiguration.getTransactionLogDir()) {
            transactionLogDirs.put(txnLogDir.getNcId(), txnLogDir.getTxnLogDirPath());
        }
        loadAsterixBuildProperties();
    }

    private AsterixConfiguration configure(String fileName) throws IOException, AsterixException {
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(fileName)) {
            if (is != null) {
                return configure(is, fileName);
            }
        }
        try (FileInputStream is = new FileInputStream(fileName)) {
            return configure(is, fileName);
        } catch (FileNotFoundException fnf1) {
            LOGGER.warn("Failed to get configuration file " + fileName + " as FileInputStream. FileNotFoundException");
            LOGGER.warn("Attempting to get default configuration file " + GlobalConfig.DEFAULT_CONFIG_FILE_NAME
                    + " as FileInputStream");
            try (FileInputStream fis = new FileInputStream(GlobalConfig.DEFAULT_CONFIG_FILE_NAME)) {
                return configure(fis, GlobalConfig.DEFAULT_CONFIG_FILE_NAME);
            } catch (FileNotFoundException fnf2) {
                fnf1.addSuppressed(fnf2);
                throw new AsterixException("Could not find configuration file " + fileName, fnf1);
            }
        }
    }

    private AsterixConfiguration configure(InputStream is, String fileName) throws AsterixException {
        try {
            JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
            Unmarshaller unmarshaller = ctx.createUnmarshaller();
            return (AsterixConfiguration) unmarshaller.unmarshal(is);
        } catch (JAXBException e) {
            throw new AsterixException("Failed to read configuration file " + fileName, e);
        }
    }

    /**
     * Constructor which wraps an IApplicationConfig.
     */
    public AsterixPropertiesAccessor(IApplicationConfig cfg) throws AsterixException {
        this.cfg = cfg;
        instanceName = cfg.getString(AsterixProperties.SECTION_ASTERIX, AsterixProperties.PROPERTY_INSTANCE_NAME,
                AsterixProperties.DEFAULT_INSTANCE_NAME);
        String mdNode = null;
        nodePartitionsMap = new HashMap<>();
        MutableInt uniquePartitionId = new MutableInt(0);
        extensions = new ArrayList<>();
        // Iterate through each configured NC.
        for (String section : cfg.getSections()) {
            if (section.startsWith(AsterixProperties.SECTION_PREFIX_NC)) {
                mdNode = configureNc(section, mdNode, uniquePartitionId);
            } else if (section.startsWith(AsterixProperties.SECTION_PREFIX_EXTENSION)) {
                String className = AsterixProperties.getSectionId(AsterixProperties.SECTION_PREFIX_EXTENSION, section);
                configureExtension(className, section);
            }
        }

        metadataNodeName = mdNode;
        asterixConfigurationParams = null;
        loadAsterixBuildProperties();
    }

    private void configureExtension(String className, String section) {
        Set<String> keys = cfg.getKeys(section);
        List<Pair<String, String>> kvs = new ArrayList<>();
        for (String key : keys) {
            String value = cfg.getString(section, key);
            kvs.add(new Pair<>(key, value));
        }
        extensions.add(new AsterixExtension(className, kvs));
    }

    private String configureNc(String section, String mdNode, MutableInt uniquePartitionId) {
        String ncId = AsterixProperties.getSectionId(AsterixProperties.SECTION_PREFIX_NC, section);
        String newMetadataNode = mdNode;

        // Here we figure out which is the metadata node. If any NCs
        // declare "metadata.port", use that one; otherwise just use the first.
        if (mdNode == null || cfg.getString(section, AsterixProperties.PROPERTY_METADATA_PORT) != null) {
            // QQQ But we don't actually *honor* metadata.port yet!
            newMetadataNode = ncId;
        }

        // Now we assign the coredump and txnlog directories for this node.
        // QQQ Default values? Should they be specified here? Or should there
        // be a default.ini? Certainly wherever they are, they should be platform-dependent.
        coredumpConfig.put(ncId, cfg.getString(section, AsterixProperties.PROPERTY_COREDUMP_DIR,
                AsterixProperties.DEFAULT_COREDUMP_DIR));
        transactionLogDirs.put(ncId,
                cfg.getString(section, AsterixProperties.PROPERTY_TXN_LOG_DIR, AsterixProperties.DEFAULT_TXN_LOG_DIR));

        // Now we create an array of ClusterPartitions for all the partitions
        // on this NC.
        String[] iodevices = cfg.getString(section, AsterixProperties.PROPERTY_IO_DEV,
                AsterixProperties.DEFAULT_IO_DEV).split(",");
        String storageSubdir = cfg.getString(section, AsterixProperties.PROPERTY_STORAGE_DIR,
                AsterixProperties.DEFAULT_STORAGE_DIR);
        String[] nodeStores = new String[iodevices.length];
        ClusterPartition[] nodePartitions = new ClusterPartition[iodevices.length];
        for (int i = 0; i < nodePartitions.length; i++) {
            // Construct final storage path from iodevice dir + storage subdir.
            nodeStores[i] = iodevices[i] + File.separator + storageSubdir;
            // Create ClusterPartition instances for this NC.
            ClusterPartition partition = new ClusterPartition(uniquePartitionId.getValue(), ncId, i);
            uniquePartitionId.increment();
            clusterPartitions.put(partition.getPartitionId(), partition);
            nodePartitions[i] = partition;
        }
        stores.put(ncId, nodeStores);
        nodePartitionsMap.put(ncId, nodePartitions);
        nodeNames.add(ncId);
        return newMetadataNode;
    }

    private void loadAsterixBuildProperties() throws AsterixException {
        Properties gitProperties = new Properties();
        try {
            gitProperties.load(getClass().getClassLoader().getResourceAsStream("git.properties"));
            for (final String name : gitProperties.stringPropertyNames()) {
                asterixBuildProperties.put(name, gitProperties.getProperty(name));
            }
        } catch (IOException e) {
            throw new AsterixException(e);
        }
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
            if (LOGGER.isEnabledFor(Level.ERROR)) {
                StringBuilder msg = new StringBuilder(
                        "Invalid property value '" + value + "' for property '" + property + "'.\n");
                if (p != null) {
                    msg.append("See the description: \n" + p.getDescription() + "\n");
                }
                msg.append("Default = " + defaultValue);
                LOGGER.error(msg.toString());
            }
            throw e;
        }
    }

    public String getInstanceName() {
        return instanceName;
    }

    public ClusterPartition getMetadataPartition() {
        // metadata partition is always the first partition on the metadata node
        return nodePartitionsMap.get(metadataNodeName)[0];
    }

    public Map<String, ClusterPartition[]> getNodePartitions() {
        return nodePartitionsMap;
    }

    public SortedMap<Integer, ClusterPartition> getClusterPartitions() {
        return clusterPartitions;
    }

    public List<AsterixExtension> getExtensions() {
        return extensions;
    }
}
