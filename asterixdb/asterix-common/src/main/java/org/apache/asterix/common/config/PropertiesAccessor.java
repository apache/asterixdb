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

import static org.apache.asterix.common.utils.StorageConstants.STORAGE_ROOT_DIR_NAME;
import static org.apache.hyracks.control.common.controllers.NCConfig.Option.IODEVICES;
import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.application.ConfigManagerApplicationConfig;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PropertiesAccessor implements IApplicationConfig {
    private static final Logger LOGGER = LogManager.getLogger();

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
        ConfigManager configManager = ((ConfigManagerApplicationConfig) cfg).getConfigManager();
        MutableInt uniquePartitionId = new MutableInt(0);
        // Iterate through each configured NC.
        for (String ncName : cfg.getNCNames()) {
            configureNc(configManager, ncName, uniquePartitionId);
        }
        for (String section : cfg.getSectionNames()) {
            if (section.startsWith(AsterixProperties.SECTION_PREFIX_EXTENSION)) {
                String className = AsterixProperties.getSectionId(AsterixProperties.SECTION_PREFIX_EXTENSION, section);
                configureExtension(className, section);
            }
        }
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
        String[] nodeStores = new String[iodevices.length];
        ClusterPartition[] nodePartitions = new ClusterPartition[iodevices.length];
        for (int i = 0; i < nodePartitions.length; i++) {
            // Construct final storage path from iodevice dir
            nodeStores[i] = joinPath(iodevices[i], STORAGE_ROOT_DIR_NAME);
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
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("Invalid property value '" + value + "' for property '" + property + "'.\n" + "Default = "
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
