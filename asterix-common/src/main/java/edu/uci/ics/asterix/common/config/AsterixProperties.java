/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.common.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import edu.uci.ics.asterix.common.configuration.AsterixConfiguration;
import edu.uci.ics.asterix.common.configuration.Coredump;
import edu.uci.ics.asterix.common.configuration.Property;
import edu.uci.ics.asterix.common.configuration.Store;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

/**
 * Holder for Asterix properties values typically set as Java Properties.
 * Intended to live in the AsterixStateProxy so it can be accessed remotely.
 */
public class AsterixProperties implements Serializable {

    private static final long serialVersionUID = 1L;
    private static String metadataNodeName;
    private static HashSet<String> nodeNames;
    private static Map<String, String[]> stores;
    private static Map<String, String> coredumpDirs;
    private static Map<String, String> asterixConfigurationParams;

    public static AsterixProperties INSTANCE = new AsterixProperties();

    public static class AsterixConfigurationKeys {

        // JVM parameters for each Node Contoller (NC)
        public static final String NC_JAVA_OPTS = "nc_java_opts";
        public static final String NC_JAVA_OPTS_DEFAULT = "-Xmx1024m";

        // JVM parameters for the Cluster Contoller (CC)
        public static final String CC_JAVA_OPTS = "cc_java_opts";
        public static final String CC_JAVA_OPTS_DEFAULT = "-Xmx1024m";

        public static final String SIZE_MEMORY_COMPONENT = "size_memory_component";
        public static final String SIZE_MEMORY_COMPONENT_DEFAULT = "512m";

        public static final String TOTAL_SIZE_MEMORY_COMPONENT = "total_size_memory_component";
        public static final String TOTAL_SIZE_MEMORY_COMPONENT_DEFAULT = "512m";

        public static final String LOG_BUFFER_NUM_PAGES = "log_buffer_num_pages";
        public static final String LOG_BUFFER_NUM_PAGES_DEFAULT = "8";

        public static final String LOG_BUFFER_PAGE_SIZE = "log_buffer_page_size";
        public static final String LOG_BUFFER_PAGE_SIZE_DEFAULT = "131072";

        public static final String LOG_PARTITION_SIZE = "log_partition_size";
        public static final String LOG_PARTITION_SIZE_DEFAULT = "2147483648";

        public static final String GROUP_COMMIT_INTERVAL = "group_commit_interval";
        public static final String GROUP_COMMIT_INTERVAL_DEFAULT = "200ms";

        public static final String SORT_OP_MEMORY = "sort_op_memory";
        public static final String SORT_OP_MEMORY_DEFAULT = "512m";

        public static final String JOIN_OP_MEMORY = "join_op_memory";
        public static final String JOIN_OP_MEMORY_DEFAULT = "512m";

        public static final String WEB_INTERFACE_PORT = "web_interface_port";
        public static final String WEB_INTERFACE_PORT_DEFAULT = "19001";

        public static final String NUM_PAGES_BUFFER_CACHE = "num_pages_buffer_cache";
        public static final String NUM_PAGES_BUFFER_CACHE_DEFAULT = "1000";

        public static final String LOG_LEVEL = "log_level";
        public static final String LOG_LEVEL_DEFAULT = "INFO";

        public static final String LSN_THRESHOLD = "lsn_threshold";
        public static final String LSN_THRESHOLD_DEFAULT = "64m";

        public static final String CHECKPOINT_TERMS_IN_SECS = "checkpoint_terms_in_secs";
        public static final String CHECKPOINT_TERMS_IN_SECS_DEFAULT = "120";

        public static final String ESCALATE_THRSHOLD_ENTITY_TO_DATASET = "escalate_threshold_entity_to_dataset";
        public static final String ESCALATE_THRSHOLD_ENTITY_TO_DATASET_DEFAULT = "8";

        public static final String SHRINK_TIMER_THRESHOLD = "shrink_timer_threshold";
        public static final String SHRINK_TIMER_THRESHOLD_DEFAULT = "120000";

        public static final String COREDUMP_PATH = "core_dump_dir";
        public static final String COREDUMP_PATH_DEFAULT = System.getProperty("user.dir");

    }

    private AsterixProperties() {
        try {
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
                    throw new AsterixException("Could not find the configuration file " + fileName);
                }
            }

            JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
            Unmarshaller unmarshaller = ctx.createUnmarshaller();
            AsterixConfiguration asterixConfiguration = (AsterixConfiguration) unmarshaller.unmarshal(is);
            metadataNodeName = asterixConfiguration.getMetadataNode();
            stores = new HashMap<String, String[]>();
            List<Store> configuredStores = asterixConfiguration.getStore();
            nodeNames = new HashSet<String>();
            for (Store store : configuredStores) {
                String trimmedStoreDirs = store.getStoreDirs().trim();
                stores.put(store.getNcId(), trimmedStoreDirs.split(","));
                nodeNames.add(store.getNcId());
            }

            coredumpDirs = new HashMap<String, String>();
            List<Coredump> configuredCoredumps = asterixConfiguration.getCoredump();
            for (Coredump coredump : configuredCoredumps) {
                coredumpDirs.put(coredump.getNcId(), coredump.getCoredumpPath().trim());
            }

            asterixConfigurationParams = new HashMap<String, String>();
            for (Property p : asterixConfiguration.getProperty()) {
                asterixConfigurationParams.put(p.getName(), p.getValue());
            }
            initializeLogLevel(getProperty(AsterixConfigurationKeys.LOG_LEVEL));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String getMetadataNodeName() {
        return metadataNodeName;
    }

    public String getMetadataStore() {
        return stores.get(metadataNodeName)[0];
    }

    public Map<String, String[]> getStores() {
        return stores;
    }

    public HashSet<String> getNodeNames() {
        return nodeNames;
    }

    public String getCoredumpPath(String ncId) {
        return coredumpDirs.get(ncId);
    }

    public String getProperty(String property) {
        String propValue = asterixConfigurationParams.get(property);
        if (propValue == null) {
            switch (property) {
                case AsterixConfigurationKeys.NC_JAVA_OPTS:
                    propValue = AsterixConfigurationKeys.NC_JAVA_OPTS_DEFAULT;
                    break;
                case AsterixConfigurationKeys.CC_JAVA_OPTS:
                    propValue = AsterixConfigurationKeys.CC_JAVA_OPTS_DEFAULT;
                    break;
                case AsterixConfigurationKeys.SIZE_MEMORY_COMPONENT:
                    propValue = AsterixConfigurationKeys.SIZE_MEMORY_COMPONENT_DEFAULT;
                    break;
                case AsterixConfigurationKeys.TOTAL_SIZE_MEMORY_COMPONENT:
                    propValue = AsterixConfigurationKeys.TOTAL_SIZE_MEMORY_COMPONENT_DEFAULT;
                    break;
                case AsterixConfigurationKeys.LOG_BUFFER_NUM_PAGES:
                    propValue = AsterixConfigurationKeys.LOG_BUFFER_NUM_PAGES_DEFAULT;
                    break;
                case AsterixConfigurationKeys.LOG_BUFFER_PAGE_SIZE:
                    propValue = AsterixConfigurationKeys.LOG_BUFFER_PAGE_SIZE_DEFAULT;
                    break;
                case AsterixConfigurationKeys.LOG_PARTITION_SIZE:
                    propValue = AsterixConfigurationKeys.LOG_PARTITION_SIZE_DEFAULT;
                    break;
                case AsterixConfigurationKeys.GROUP_COMMIT_INTERVAL:
                    propValue = AsterixConfigurationKeys.GROUP_COMMIT_INTERVAL_DEFAULT;
                    break;
                case AsterixConfigurationKeys.SORT_OP_MEMORY:
                    propValue = AsterixConfigurationKeys.SORT_OP_MEMORY_DEFAULT;
                    break;
                case AsterixConfigurationKeys.JOIN_OP_MEMORY:
                    propValue = AsterixConfigurationKeys.JOIN_OP_MEMORY_DEFAULT;
                    break;
                case AsterixConfigurationKeys.WEB_INTERFACE_PORT:
                    propValue = AsterixConfigurationKeys.WEB_INTERFACE_PORT_DEFAULT;
                    break;
                case AsterixConfigurationKeys.NUM_PAGES_BUFFER_CACHE:
                    propValue = AsterixConfigurationKeys.NUM_PAGES_BUFFER_CACHE_DEFAULT;
                    break;
                case AsterixConfigurationKeys.LOG_LEVEL:
                    propValue = AsterixConfigurationKeys.LOG_LEVEL_DEFAULT;
                    break;
                case AsterixConfigurationKeys.LSN_THRESHOLD:
                    propValue = AsterixConfigurationKeys.LSN_THRESHOLD_DEFAULT;
                    break;
                case AsterixConfigurationKeys.CHECKPOINT_TERMS_IN_SECS:
                    propValue = AsterixConfigurationKeys.CHECKPOINT_TERMS_IN_SECS_DEFAULT;
                    break;
                case AsterixConfigurationKeys.ESCALATE_THRSHOLD_ENTITY_TO_DATASET:
                    propValue = AsterixConfigurationKeys.ESCALATE_THRSHOLD_ENTITY_TO_DATASET_DEFAULT;
                    break;
                case AsterixConfigurationKeys.SHRINK_TIMER_THRESHOLD:
                    propValue = AsterixConfigurationKeys.SHRINK_TIMER_THRESHOLD_DEFAULT;
                    break;
            }
        }
        return propValue;
    }

    private void initializeLogLevel(String configuredLogLevel) {
        Level level = null;
        switch (configuredLogLevel.toLowerCase()) {
            case "info":
                level = Level.INFO;
                break;
            case "fine":
                level = Level.FINE;
                break;
            case "finer":
                level = Level.FINER;
                break;
            case "finest":
                level = Level.FINEST;
                break;
            case "severe":
                level = Level.SEVERE;
                break;
            case "off":
                level = Level.OFF;
                break;
            case "warning":
                level = Level.WARNING;
                break;
            default:  
                level = Level.ALL;
        }
        Logger.getLogger("edu.uci.ics").setLevel(level);
    }
}
