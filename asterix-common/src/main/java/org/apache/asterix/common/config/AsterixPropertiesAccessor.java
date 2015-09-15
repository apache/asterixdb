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
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.common.configuration.AsterixConfiguration;
import org.apache.asterix.common.configuration.Coredump;
import org.apache.asterix.common.configuration.Property;
import org.apache.asterix.common.configuration.Store;
import org.apache.asterix.common.configuration.TransactionLogDir;
import org.apache.asterix.common.exceptions.AsterixException;

public class AsterixPropertiesAccessor {
    private static final Logger LOGGER = Logger.getLogger(AsterixPropertiesAccessor.class.getName());

    private final String instanceName;
    private final String metadataNodeName;
    private final Set<String> nodeNames;
    private final Map<String, String[]> stores;
    private final Map<String, String> coredumpConfig;
    private final Map<String, Property> asterixConfigurationParams;
    private final Map<String, String> transactionLogDirs;

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
        try {
            JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
            Unmarshaller unmarshaller = ctx.createUnmarshaller();
            asterixConfiguration = (AsterixConfiguration) unmarshaller.unmarshal(is);
        } catch (JAXBException e) {
            throw new AsterixException("Failed to read configuration file " + fileName);
        }
        instanceName = asterixConfiguration.getInstanceName();
        metadataNodeName = asterixConfiguration.getMetadataNode();
        stores = new HashMap<String, String[]>();
        List<Store> configuredStores = asterixConfiguration.getStore();
        nodeNames = new HashSet<String>();
        for (Store store : configuredStores) {
            String trimmedStoreDirs = store.getStoreDirs().trim();
            stores.put(store.getNcId(), trimmedStoreDirs.split(","));
            nodeNames.add(store.getNcId());
        }
        asterixConfigurationParams = new HashMap<String, Property>();
        for (Property p : asterixConfiguration.getProperty()) {
            asterixConfigurationParams.put(p.getName(), p);
        }
        coredumpConfig = new HashMap<String, String>();
        for (Coredump cd : asterixConfiguration.getCoredump()) {
            coredumpConfig.put(cd.getNcId(), cd.getCoredumpPath());
        }
        transactionLogDirs = new HashMap<String, String>();
        for (TransactionLogDir txnLogDir : asterixConfiguration.getTransactionLogDir()) {
            transactionLogDirs.put(txnLogDir.getNcId(), txnLogDir.getTxnLogDirPath());
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

    public Set<String> getNodeNames() {
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
        Property p = asterixConfigurationParams.get(property);
        if (p == null) {
            return defaultValue;
        }

        try {
            return interpreter.interpret(p);
        } catch (IllegalArgumentException e) {
            logConfigurationError(p, defaultValue);
            throw e;
        }
    }

    private <T> void logConfigurationError(Property p, T defaultValue) {
        if (LOGGER.isLoggable(Level.SEVERE)) {
            LOGGER.severe("Invalid property value '" + p.getValue() + "' for property '" + p.getName()
                    + "'.\n See the description: \n" + p.getDescription() + "\nDefault = " + defaultValue);
        }
    }

    public String getInstanceName() {
        return instanceName;
    }
}
