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

import static org.apache.asterix.common.config.MetadataProperties.Option.INSTANCE_NAME;
import static org.apache.asterix.common.config.MetadataProperties.Option.METADATA_NODE;
import static org.apache.asterix.common.config.NodeProperties.Option.STORAGE_SUBDIR;
import static org.apache.hyracks.control.common.controllers.NCConfig.Option.IODEVICES;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

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
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.utils.ConfigUtil;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.application.ConfigManagerApplicationConfig;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;

public class PropertiesAccessor implements IApplicationConfig {
    private static final Logger LOGGER = Logger.getLogger(PropertiesAccessor.class.getName());

    private static final Map<IApplicationConfig, PropertiesAccessor> instances = new ConcurrentHashMap<>();
    private final Map<String, String[]> stores = new HashMap<>();
    private final Map<String, String> coredumpConfig = new HashMap<>();
    private final IApplicationConfig cfg;
    private final Map<String, String> transactionLogDirs = new HashMap<>();
    private final Map<String, String> asterixBuildProperties = new HashMap<>();
    private final Map<String, ClusterPartition[]> nodePartitionsMap;
    private final SortedMap<Integer, ClusterPartition> clusterPartitions;
    // For extensions
    private final List<AsterixExtension> extensions;

    /**
     * Constructor which wraps an IApplicationConfig.
     */
    private PropertiesAccessor(IApplicationConfig cfg) throws AsterixException, IOException {
        this.cfg = cfg;
        nodePartitionsMap = new ConcurrentHashMap<>();
        clusterPartitions = Collections.synchronizedSortedMap(new TreeMap<>());
        extensions = new ArrayList<>();
        // Determine whether to use old-style asterix-configuration.xml or new-style configuration.
        // QQQ strip this out eventually
        // QQQ this is NOT a good way to determine whether to use config file
        ConfigManager configManager = ((ConfigManagerApplicationConfig) cfg).getConfigManager();
        boolean usingConfigFile = Stream
                .of((IOption) ControllerConfig.Option.CONFIG_FILE, ControllerConfig.Option.CONFIG_FILE_URL)
                .map(configManager::get).anyMatch(Objects::nonNull);
        AsterixConfiguration asterixConfiguration = null;
        try {
            asterixConfiguration = configure(
                    System.getProperty(GlobalConfig.CONFIG_FILE_PROPERTY, GlobalConfig.DEFAULT_CONFIG_FILE_NAME));
        } catch (Exception e) {
            // cannot load config file, assume new-style config
        }

        if (!usingConfigFile && asterixConfiguration != null) {
            LOGGER.info("using old-style configuration: " + System.getProperty(GlobalConfig.CONFIG_FILE_PROPERTY));
            if (asterixConfiguration.getInstanceName() != null) {
                configManager.set(INSTANCE_NAME, asterixConfiguration.getInstanceName());
            }
            if (asterixConfiguration.getMetadataNode() != null) {
                configManager.set(METADATA_NODE, asterixConfiguration.getMetadataNode());
            }
            List<Store> configuredStores = asterixConfiguration.getStore();

            int uniquePartitionId = 0;
            // Here we iterate through all <store> elements in asterix-configuration.xml.
            // For each one, we create an array of ClusterPartitions and store this array
            // in nodePartitionsMap, keyed by the node name. The array is the same length
            // as the comma-separated <storeDirs> child element, because Managix will have
            // arranged for that element to be populated with the full paths to each
            // partition directory (as formed by appending the <store> subdirectory to
            // each <iodevices> path from the user's original cluster.xml).
            for (Store store : configuredStores) {
                configManager.set(store.getNcId(), NodeProperties.Option.STARTING_PARTITION_ID, uniquePartitionId);
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
                configManager.registerVirtualNode(store.getNcId());
                // push the store info to the config manager
                configManager.set(store.getNcId(), NCConfig.Option.IODEVICES, nodeStores);
                // marking node as virtual, as we're not using NCServices with old-style config
                configManager.set(store.getNcId(), NCConfig.Option.VIRTUAL_NC, true);
            }
            // Get extensions
            if (asterixConfiguration.getExtensions() != null) {
                for (Extension ext : asterixConfiguration.getExtensions().getExtension()) {
                    extensions.add(ConfigUtil.toAsterixExtension(ext));
                }
            }
            for (Property p : asterixConfiguration.getProperty()) {
                IOption option = null;
                for (Section section : Arrays.asList(Section.COMMON, Section.CC, Section.NC)) {
                    IOption optionTemp = cfg.lookupOption(section.sectionName(), p.getName());
                    if (optionTemp == null) {
                        continue;
                    }
                    if (option != null) {
                        throw new IllegalStateException(
                                "ERROR: option found in multiple sections: " + Arrays.asList(option, optionTemp));
                    }
                    option = optionTemp;
                }
                if (option == null) {
                    LOGGER.warning("Ignoring unknown property: " + p.getName());
                } else {
                    configManager.set(option, option.type().parse(p.getValue()));
                }
            }
            for (Coredump cd : asterixConfiguration.getCoredump()) {
                coredumpConfig.put(cd.getNcId(), cd.getCoredumpPath());
            }
            for (TransactionLogDir txnLogDir : asterixConfiguration.getTransactionLogDir()) {
                transactionLogDirs.put(txnLogDir.getNcId(), txnLogDir.getTxnLogDirPath());
            }
        } else {
            LOGGER.info("using new-style configuration");
            MutableInt uniquePartitionId = new MutableInt(0);
            // Iterate through each configured NC.
            for (String ncName : cfg.getNCNames()) {
                configureNc(configManager, ncName, uniquePartitionId);
            }
            for (String section : cfg.getSectionNames()) {
                if (section.startsWith(AsterixProperties.SECTION_PREFIX_EXTENSION)) {
                    String className = AsterixProperties.getSectionId(AsterixProperties.SECTION_PREFIX_EXTENSION,
                            section);
                    configureExtension(className, section);
                }
            }
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
            LOGGER.warning(
                    "Failed to get configuration file " + fileName + " as FileInputStream. FileNotFoundException");
            LOGGER.warning("Attempting to get default configuration file " + GlobalConfig.DEFAULT_CONFIG_FILE_NAME
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

    private void configureExtension(String className, String section) {
        Set<String> keys = cfg.getKeys(section);
        List<Pair<String, String>> kvs = new ArrayList<>();
        for (String key : keys) {
            String value = cfg.getString(section, key);
            kvs.add(new Pair<>(key, value));
        }
        extensions.add(new AsterixExtension(className, kvs));
    }

    private void configureNc(ConfigManager configManager, String ncId, MutableInt uniquePartitionId)
            throws AsterixException {

        // Now we assign the coredump and txnlog directories for this node.
        // QQQ Default values? Should they be specified here? Or should there
        // be a default.ini? Certainly wherever they are, they should be platform-dependent.
        IApplicationConfig nodeCfg = cfg.getNCEffectiveConfig(ncId);
        coredumpConfig.put(ncId, nodeCfg.getString(NodeProperties.Option.CORE_DUMP_DIR));
        transactionLogDirs.put(ncId, nodeCfg.getString(NodeProperties.Option.TXN_LOG_DIR));
        int partitionId = nodeCfg.getInt(NodeProperties.Option.STARTING_PARTITION_ID);
        if (partitionId != -1) {
            uniquePartitionId.setValue(partitionId);
        } else {
            configManager.set(ncId, NodeProperties.Option.STARTING_PARTITION_ID, uniquePartitionId.getValue());
        }

        // Now we create an array of ClusterPartitions for all the partitions
        // on this NC.
        String[] iodevices = nodeCfg.getStringArray(IODEVICES);
        String storageSubdir = nodeCfg.getString(STORAGE_SUBDIR);
        String[] nodeStores = new String[iodevices.length];
        ClusterPartition[] nodePartitions = new ClusterPartition[iodevices.length];
        for (int i = 0; i < nodePartitions.length; i++) {
            // Construct final storage path from iodevice dir + storage subdirs
            nodeStores[i] = iodevices[i] + File.separator + storageSubdir;
            // Create ClusterPartition instances for this NC.
            ClusterPartition partition = new ClusterPartition(uniquePartitionId.getAndIncrement(), ncId, i);
            ClusterPartition orig = clusterPartitions.put(partition.getPartitionId(), partition);
            if (orig != null) {
                throw AsterixException.create(ErrorCode.DUPLICATE_PARTITION_ID, partition.getPartitionId(), ncId,
                        orig.getNodeId());
            }
            nodePartitions[i] = partition;
        }
        stores.put(ncId, nodeStores);
        nodePartitionsMap.put(ncId, nodePartitions);
    }

    private void loadAsterixBuildProperties() throws AsterixException {
        Properties gitProperties = new Properties();
        try {
            InputStream propertyStream = getClass().getClassLoader().getResourceAsStream("git.properties");
            if (propertyStream != null) {
                gitProperties.load(propertyStream);
                for (final String name : gitProperties.stringPropertyNames()) {
                    asterixBuildProperties.put(name, gitProperties.getProperty(name));
                }
            } else {
                LOGGER.info("Build properties not found on classpath. Version API will return empty object");
            }
        } catch (IOException e) {
            throw new AsterixException(e);
        }
    }

    public Map<String, String[]> getStores() {
        return stores;
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

    public <T> T getProperty(String property, T defaultValue, IOptionType<T> interpreter) {
        String value = cfg.getString("common", property);
        try {
            return value == null ? defaultValue : interpreter.parse(value);
        } catch (IllegalArgumentException e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("Invalid property value '" + value + "' for property '" + property + "'.\n" + "Default = "
                        + defaultValue);
            }
            throw e;
        }
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

    public static PropertiesAccessor getInstance(IApplicationConfig cfg) throws IOException, AsterixException {
        PropertiesAccessor accessor = instances.get(cfg);
        if (accessor == null) {
            accessor = new PropertiesAccessor(cfg);
            if (instances.putIfAbsent(cfg, accessor) != null) {
                return instances.get(cfg);
            }
        }
        return accessor;
    }

    @Override
    public Object getStatic(IOption option) {
        return cfg.getStatic(option);
    }

    @Override
    public String getString(String section, String key) {
        return cfg.getString(section, key);
    }

    @Override
    public int getInt(String section, String key) {
        return cfg.getInt(section, key);
    }

    @Override
    public long getLong(String section, String key) {
        return cfg.getLong(section, key);
    }

    @Override
    public Set<String> getSectionNames() {
        return cfg.getSectionNames();
    }

    @Override
    public Set<String> getKeys(String section) {
        return cfg.getKeys(section);
    }

    @Override
    public List<String> getNCNames() {
        return cfg.getNCNames();
    }

    @Override
    public IOption lookupOption(String sectionName, String propertyName) {
        return cfg.lookupOption(sectionName, propertyName);
    }

    @Override
    public IApplicationConfig getNCEffectiveConfig(String nodeId) {
        return cfg.getNCEffectiveConfig(nodeId);
    }

    @Override
    public Set<IOption> getOptions() {
        return cfg.getOptions();
    }

    @Override
    public Set<IOption> getOptions(Section section) {
        return cfg.getOptions(section);
    }

    @Override
    public Set<Section> getSections() {
        return cfg.getSections();
    }

    @Override
    public Set<Section> getSections(Predicate<Section> predicate) {
        return cfg.getSections(predicate);
    }
}
