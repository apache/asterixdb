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
package org.apache.hyracks.control.common.controllers;

import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.LONG;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING_ARRAY;
import static org.apache.hyracks.control.common.config.OptionTypes.UNSIGNED_INTEGER;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.util.file.FileUtil;

public class NCConfig extends ControllerConfig {
    private static final long serialVersionUID = 3L;

    public enum Option implements IOption {
        ADDRESS(STRING, InetAddress.getLoopbackAddress().getHostAddress()),
        PUBLIC_ADDRESS(STRING, ADDRESS),
        CLUSTER_LISTEN_ADDRESS(STRING, ADDRESS),
        CLUSTER_LISTEN_PORT(UNSIGNED_INTEGER, 0),
        NCSERVICE_ADDRESS(STRING, PUBLIC_ADDRESS),
        NCSERVICE_PORT(INTEGER, 9090),
        CLUSTER_ADDRESS(STRING, (String) null),
        CLUSTER_PORT(UNSIGNED_INTEGER, 1099),
        CLUSTER_PUBLIC_ADDRESS(STRING, PUBLIC_ADDRESS),
        CLUSTER_PUBLIC_PORT(UNSIGNED_INTEGER, CLUSTER_LISTEN_PORT),
        NODE_ID(STRING, (String) null),
        DATA_LISTEN_ADDRESS(STRING, ADDRESS),
        DATA_LISTEN_PORT(UNSIGNED_INTEGER, 0),
        DATA_PUBLIC_ADDRESS(STRING, PUBLIC_ADDRESS),
        DATA_PUBLIC_PORT(UNSIGNED_INTEGER, DATA_LISTEN_PORT),
        RESULT_LISTEN_ADDRESS(STRING, ADDRESS),
        RESULT_LISTEN_PORT(UNSIGNED_INTEGER, 0),
        RESULT_PUBLIC_ADDRESS(STRING, PUBLIC_ADDRESS),
        RESULT_PUBLIC_PORT(UNSIGNED_INTEGER, RESULT_LISTEN_PORT),
        MESSAGING_LISTEN_ADDRESS(STRING, ADDRESS),
        MESSAGING_LISTEN_PORT(UNSIGNED_INTEGER, 0),
        MESSAGING_PUBLIC_ADDRESS(STRING, PUBLIC_ADDRESS),
        MESSAGING_PUBLIC_PORT(UNSIGNED_INTEGER, MESSAGING_LISTEN_PORT),
        REPLICATION_LISTEN_ADDRESS(STRING, ADDRESS),
        REPLICATION_LISTEN_PORT(UNSIGNED_INTEGER, 2000),
        REPLICATION_PUBLIC_ADDRESS(STRING, PUBLIC_ADDRESS),
        REPLICATION_PUBLIC_PORT(UNSIGNED_INTEGER, REPLICATION_LISTEN_PORT),
        CLUSTER_CONNECT_RETRIES(UNSIGNED_INTEGER, 5),
        IODEVICES(
                STRING_ARRAY,
                appConfig -> new String[] {
                        FileUtil.joinPath(appConfig.getString(ControllerConfig.Option.DEFAULT_DIR), "iodevice") },
                "<value of " + ControllerConfig.Option.DEFAULT_DIR.cmdline() + ">/iodevice"),
        NET_THREAD_COUNT(POSITIVE_INTEGER, 1),
        NET_BUFFER_COUNT(POSITIVE_INTEGER, 1),
        RESULT_TTL(LONG, 86400000L),
        RESULT_SWEEP_THRESHOLD(LONG, 60000L),
        RESULT_MANAGER_MEMORY(INTEGER_BYTE_UNIT, -1),
        @SuppressWarnings("RedundantCast") // not redundant- false positive from IDEA
        APP_CLASS(STRING, (String) null),
        NCSERVICE_PID(INTEGER, -1),
        COMMAND(STRING, "hyracksnc"),
        JVM_ARGS(STRING, (String) null),
        TRACE_CATEGORIES(STRING_ARRAY, new String[0]),
        KEY_STORE_PATH(STRING, (String) null),
        TRUST_STORE_PATH(STRING, (String) null),
        KEY_STORE_PASSWORD(STRING, (String) null),
        IO_WORKERS_PER_PARTITION(POSITIVE_INTEGER, 2),
        IO_QUEUE_SIZE(POSITIVE_INTEGER, 10);

        private final IOptionType parser;
        private final String defaultValueDescription;
        private Object defaultValue;

        <T> Option(IOptionType<T> parser, Option defaultOption) {
            this.parser = parser;
            this.defaultValue = defaultOption;
            defaultValueDescription = null;
        }

        <T> Option(IOptionType<T> parser, T defaultValue) {
            this.parser = parser;
            this.defaultValue = defaultValue;
            defaultValueDescription = null;
        }

        <T> Option(IOptionType<T> parser, Function<IApplicationConfig, T> defaultValue,
                String defaultValueDescription) {
            this.parser = parser;
            this.defaultValue = defaultValue;
            this.defaultValueDescription = defaultValueDescription;
        }

        @Override
        public Section section() {
            switch (this) {
                case NODE_ID:
                    return Section.LOCALNC;
                default:
                    return Section.NC;
            }
        }

        @Override
        @SuppressWarnings("squid:MethodCyclomaticComplexity")
        public String description() {
            switch (this) {
                case ADDRESS:
                    return "Default IP Address to bind listeners on this NC.  All services will bind on this address "
                            + "unless a service-specific listen address is supplied.";
                case CLUSTER_LISTEN_ADDRESS:
                    return "IP Address to bind cluster listener on this NC";
                case PUBLIC_ADDRESS:
                    return "Default public address that other processes should use to contact this NC.  All services "
                            + "will advertise this address unless a service-specific public address is supplied.";
                case NCSERVICE_ADDRESS:
                    return "Address the CC should use to contact the NCService associated with this NC";
                case NCSERVICE_PORT:
                    return "Port the CC should use to contact the NCService associated with this NC (-1 to not use "
                            + "NCService to start this NC)";
                case CLUSTER_ADDRESS:
                    return "Cluster Controller address (required unless specified in config file)";
                case CLUSTER_PORT:
                    return "Cluster Controller port";
                case CLUSTER_LISTEN_PORT:
                    return "IP port to bind cluster listener";
                case CLUSTER_PUBLIC_ADDRESS:
                    return "Public IP Address to announce cluster listener";
                case CLUSTER_PUBLIC_PORT:
                    return "Public IP port to announce cluster listener";
                case NODE_ID:
                    return "Logical name of node controller unique within the cluster (required unless specified in "
                            + "config file)";
                case DATA_LISTEN_ADDRESS:
                    return "IP Address to bind data listener";
                case DATA_LISTEN_PORT:
                    return "IP port to bind data listener";
                case DATA_PUBLIC_ADDRESS:
                    return "Public IP Address to announce data listener";
                case DATA_PUBLIC_PORT:
                    return "Public IP port to announce data listener";
                case RESULT_LISTEN_ADDRESS:
                    return "IP Address to bind result distribution listener";
                case RESULT_LISTEN_PORT:
                    return "IP port to bind result distribution listener";
                case RESULT_PUBLIC_ADDRESS:
                    return "Public IP Address to announce result distribution listener";
                case RESULT_PUBLIC_PORT:
                    return "Public IP port to announce result distribution listener";
                case MESSAGING_LISTEN_ADDRESS:
                    return "IP Address to bind messaging listener";
                case MESSAGING_LISTEN_PORT:
                    return "IP port to bind messaging listener";
                case MESSAGING_PUBLIC_ADDRESS:
                    return "Public IP Address to announce messaging listener";
                case MESSAGING_PUBLIC_PORT:
                    return "Public IP port to announce messaging listener";
                case REPLICATION_PUBLIC_ADDRESS:
                    return "Public address to advertise for replication service";
                case REPLICATION_PUBLIC_PORT:
                    return "Public port to advertise for replication service";
                case REPLICATION_LISTEN_ADDRESS:
                    return "Replication bind address";
                case REPLICATION_LISTEN_PORT:
                    return "Port to listen on for replication service";
                case CLUSTER_CONNECT_RETRIES:
                    return "Number of attempts to retry contacting CC before giving up";
                case IODEVICES:
                    return "Comma separated list of IO Device mount points";
                case NET_THREAD_COUNT:
                    return "Number of threads to use for Network I/O";
                case NET_BUFFER_COUNT:
                    return "Number of network buffers per input/output channel";
                case RESULT_TTL:
                    return "Limits the amount of time results for asynchronous jobs should be retained by the system "
                            + "in milliseconds";
                case RESULT_SWEEP_THRESHOLD:
                    return "The duration within which an instance of the result cleanup should be invoked in "
                            + "milliseconds";
                case RESULT_MANAGER_MEMORY:
                    return "Memory usable for result caching at this Node Controller in bytes";
                case APP_CLASS:
                    return "Application NC Main Class";
                case NCSERVICE_PID:
                    return "PID of the NCService which launched this NCDriver";
                case COMMAND:
                    return "Command NCService should invoke to start the NCDriver";
                case JVM_ARGS:
                    return "JVM args to pass to the NCDriver";
                case TRACE_CATEGORIES:
                    return "Categories for tracing";
                case KEY_STORE_PATH:
                    return "A fully-qualified path to a key store file that will be used for secured connections";
                case TRUST_STORE_PATH:
                    return "A fully-qualified path to a trust store file that will be used for secured connections";
                case KEY_STORE_PASSWORD:
                    return "The password to the provided key store";
                case IO_WORKERS_PER_PARTITION:
                    return "Number of threads per partition used to write and read from storage";
                case IO_QUEUE_SIZE:
                    return "Length of the queue used for requests to write and read";
                default:
                    throw new IllegalStateException("Not yet implemented: " + this);
            }
        }

        @Override
        public IOptionType type() {
            return parser;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
        }

        @Override
        public String usageDefaultOverride(IApplicationConfig accessor, Function<IOption, String> optionPrinter) {
            return defaultValueDescription;
        }

        @Override
        public boolean hidden() {
            return this == KEY_STORE_PASSWORD;
        }
    }

    public String getReplicationPublicAddress() {
        return appConfig.getString(Option.REPLICATION_LISTEN_ADDRESS);
    }

    public static final int NCSERVICE_PORT_DISABLED = -1;

    private List<String> appArgs = new ArrayList<>();

    private final IApplicationConfig appConfig;
    private final String nodeId;

    public NCConfig(String nodeId) {
        this(nodeId, new ConfigManager(null));
    }

    public NCConfig(String nodeId, ConfigManager configManager) {
        this(nodeId, configManager, true);
    }

    public NCConfig(String nodeId, ConfigManager configManager, boolean selfRegister) {
        super(configManager);
        this.appConfig = nodeId == null ? configManager.getAppConfig() : configManager.getNodeEffectiveConfig(nodeId);
        if (selfRegister) {
            configManager.register(Option.class);
            configManager.register(ControllerConfig.Option.class);
        } else {
            configManager.register(Option.NODE_ID);
        }
        setNodeId(nodeId);
        this.nodeId = nodeId;
        configManager.registerArgsListener(appArgs::addAll);
    }

    public List<String> getAppArgs() {
        return appArgs;
    }

    public String[] getAppArgsArray() {
        return appArgs.toArray(new String[appArgs.size()]);
    }

    public IApplicationConfig getNodeScopedAppConfig() {
        return appConfig;
    }

    public String getPublicAddress() {
        return appConfig.getString(Option.PUBLIC_ADDRESS);
    }

    public void setPublicAddress(String publicAddress) {
        configManager.set(nodeId, Option.PUBLIC_ADDRESS, publicAddress);
    }

    public String getNCServiceAddress() {
        return appConfig.getString(Option.NCSERVICE_ADDRESS);
    }

    public void setNCServiceAddress(String ncserviceAddress) {
        configManager.set(nodeId, Option.NCSERVICE_ADDRESS, ncserviceAddress);
    }

    public int getNCServicePort() {
        return appConfig.getInt(Option.NCSERVICE_PORT);
    }

    public void setNCServicePort(int ncservicePort) {
        configManager.set(nodeId, Option.NCSERVICE_PORT, ncservicePort);
    }

    public String getClusterAddress() {
        return appConfig.getString(Option.CLUSTER_ADDRESS);
    }

    public void setClusterAddress(String clusterAddress) {
        configManager.set(nodeId, Option.CLUSTER_ADDRESS, clusterAddress);
    }

    public int getClusterPort() {
        return appConfig.getInt(Option.CLUSTER_PORT);
    }

    public void setClusterPort(int clusterPort) {
        configManager.set(nodeId, Option.CLUSTER_PORT, clusterPort);
    }

    public String getClusterListenAddress() {
        return appConfig.getString(Option.CLUSTER_LISTEN_ADDRESS);
    }

    public void setClusterListenAddress(String clusterListenAddress) {
        configManager.set(nodeId, Option.CLUSTER_LISTEN_ADDRESS, clusterListenAddress);
    }

    public int getClusterListenPort() {
        return appConfig.getInt(Option.CLUSTER_LISTEN_PORT);
    }

    public void setClusterListenPort(int clusterListenPort) {
        configManager.set(nodeId, Option.CLUSTER_LISTEN_PORT, clusterListenPort);
    }

    public String getClusterPublicAddress() {
        return appConfig.getString(Option.CLUSTER_PUBLIC_ADDRESS);
    }

    public void setClusterPublicAddress(String clusterPublicAddress) {
        configManager.set(nodeId, Option.CLUSTER_PUBLIC_ADDRESS, clusterPublicAddress);
    }

    public int getClusterPublicPort() {
        return appConfig.getInt(Option.CLUSTER_PUBLIC_PORT);
    }

    public void setClusterPublicPort(int clusterPublicPort) {
        configManager.set(nodeId, Option.CLUSTER_PUBLIC_PORT, clusterPublicPort);
    }

    public String getNodeId() {
        return appConfig.getString(Option.NODE_ID);
    }

    public void setNodeId(String nodeId) {
        configManager.set(nodeId, Option.NODE_ID, nodeId);
    }

    public String getDataListenAddress() {
        return appConfig.getString(Option.DATA_LISTEN_ADDRESS);
    }

    public void setDataListenAddress(String dataListenAddress) {
        configManager.set(nodeId, Option.DATA_LISTEN_ADDRESS, dataListenAddress);
    }

    public int getDataListenPort() {
        return appConfig.getInt(Option.DATA_LISTEN_PORT);
    }

    public void setDataListenPort(int dataListenPort) {
        configManager.set(nodeId, Option.DATA_LISTEN_PORT, dataListenPort);
    }

    public String getDataPublicAddress() {
        return appConfig.getString(Option.DATA_PUBLIC_ADDRESS);
    }

    public void setDataPublicAddress(String dataPublicAddress) {
        configManager.set(nodeId, Option.DATA_PUBLIC_ADDRESS, dataPublicAddress);
    }

    public int getDataPublicPort() {
        return appConfig.getInt(Option.DATA_PUBLIC_PORT);
    }

    public void setDataPublicPort(int dataPublicPort) {
        configManager.set(nodeId, Option.DATA_PUBLIC_PORT, dataPublicPort);
    }

    public String getResultListenAddress() {
        return appConfig.getString(Option.RESULT_LISTEN_ADDRESS);
    }

    public void setResultListenAddress(String resultListenAddress) {
        configManager.set(nodeId, Option.RESULT_LISTEN_ADDRESS, resultListenAddress);
    }

    public int getResultListenPort() {
        return appConfig.getInt(Option.RESULT_LISTEN_PORT);
    }

    public void setResultListenPort(int resultListenPort) {
        configManager.set(nodeId, Option.RESULT_LISTEN_PORT, resultListenPort);
    }

    public String getResultPublicAddress() {
        return appConfig.getString(Option.RESULT_PUBLIC_ADDRESS);
    }

    public void setResultPublicAddress(String resultPublicAddress) {
        configManager.set(nodeId, Option.RESULT_PUBLIC_ADDRESS, resultPublicAddress);
    }

    public int getResultPublicPort() {
        return appConfig.getInt(Option.RESULT_PUBLIC_PORT);
    }

    public void setResultPublicPort(int resultPublicPort) {
        configManager.set(nodeId, Option.RESULT_PUBLIC_PORT, resultPublicPort);
    }

    public String getMessagingListenAddress() {
        return appConfig.getString(Option.MESSAGING_LISTEN_ADDRESS);
    }

    public void setMessagingListenAddress(String messagingListenAddress) {
        configManager.set(nodeId, Option.MESSAGING_LISTEN_ADDRESS, messagingListenAddress);
    }

    public int getMessagingListenPort() {
        return appConfig.getInt(Option.MESSAGING_LISTEN_PORT);
    }

    public void setMessagingListenPort(int messagingListenPort) {
        configManager.set(nodeId, Option.MESSAGING_LISTEN_PORT, messagingListenPort);
    }

    public String getMessagingPublicAddress() {
        return appConfig.getString(Option.MESSAGING_PUBLIC_ADDRESS);
    }

    public void setMessagingPublicAddress(String messagingPublicAddress) {
        configManager.set(nodeId, Option.MESSAGING_PUBLIC_ADDRESS, messagingPublicAddress);
    }

    public int getMessagingPublicPort() {
        return appConfig.getInt(Option.MESSAGING_PUBLIC_PORT);
    }

    public void setMessagingPublicPort(int messagingPublicPort) {
        configManager.set(nodeId, Option.MESSAGING_PUBLIC_PORT, messagingPublicPort);
    }

    public int getReplicationPublicPort() {
        return appConfig.getInt(Option.REPLICATION_LISTEN_PORT);
    }

    public int getClusterConnectRetries() {
        return appConfig.getInt(Option.CLUSTER_CONNECT_RETRIES);
    }

    public void setClusterConnectRetries(int clusterConnectRetries) {
        configManager.set(nodeId, Option.CLUSTER_CONNECT_RETRIES, clusterConnectRetries);
    }

    public String[] getIODevices() {
        return appConfig.getStringArray(Option.IODEVICES);
    }

    public void setIODevices(String[] iodevices) {
        configManager.set(nodeId, Option.IODEVICES, iodevices);
    }

    public String[] getTraceCategories() {
        return appConfig.getStringArray(Option.TRACE_CATEGORIES);
    }

    public void setTraceCategories(String[] traceCategories) {
        configManager.set(nodeId, Option.TRACE_CATEGORIES, traceCategories);
    }

    public int getNetThreadCount() {
        return appConfig.getInt(Option.NET_THREAD_COUNT);
    }

    public void setNetThreadCount(int netThreadCount) {
        configManager.set(nodeId, Option.NET_THREAD_COUNT, netThreadCount);
    }

    public int getNetBufferCount() {
        return appConfig.getInt(Option.NET_BUFFER_COUNT);
    }

    public void setNetBufferCount(int netBufferCount) {
        configManager.set(nodeId, Option.NET_BUFFER_COUNT, netBufferCount);
    }

    public long getResultTTL() {
        return appConfig.getLong(Option.RESULT_TTL);
    }

    public void setResultTTL(long resultTTL) {
        configManager.set(nodeId, Option.RESULT_TTL, resultTTL);
    }

    public long getResultSweepThreshold() {
        return appConfig.getLong(Option.RESULT_SWEEP_THRESHOLD);
    }

    public void setResultSweepThreshold(long resultSweepThreshold) {
        configManager.set(nodeId, Option.RESULT_SWEEP_THRESHOLD, resultSweepThreshold);
    }

    public int getResultManagerMemory() {
        return appConfig.getInt(Option.RESULT_MANAGER_MEMORY);
    }

    public void setResultManagerMemory(int resultManagerMemory) {
        configManager.set(nodeId, Option.RESULT_MANAGER_MEMORY, resultManagerMemory);
    }

    public String getAppClass() {
        return appConfig.getString(Option.APP_CLASS);
    }

    public void setAppClass(String appClass) {
        configManager.set(nodeId, Option.APP_CLASS, appClass);
    }

    public int getNCServicePid() {
        return appConfig.getInt(Option.NCSERVICE_PID);
    }

    public void setNCServicePid(int ncservicePid) {
        configManager.set(nodeId, Option.NCSERVICE_PID, ncservicePid);
    }

    public boolean isVirtualNC() {
        return appConfig.getInt(NCConfig.Option.NCSERVICE_PORT) == NCConfig.NCSERVICE_PORT_DISABLED;
    }

    public void setVirtualNC() {
        configManager.set(nodeId, Option.NCSERVICE_PORT, NCSERVICE_PORT_DISABLED);
    }

    public String getKeyStorePath() {
        return appConfig.getString(Option.KEY_STORE_PATH);
    }

    public String getKeyStorePassword() {
        return appConfig.getString(Option.KEY_STORE_PASSWORD);
    }

    public void setKeyStorePath(String keyStorePath) {
        configManager.set(nodeId, Option.KEY_STORE_PATH, keyStorePath);
    }

    public String getTrustStorePath() {
        return appConfig.getString(Option.TRUST_STORE_PATH);
    }

    public void setTrustStorePath(String keyStorePath) {
        configManager.set(nodeId, Option.TRUST_STORE_PATH, keyStorePath);
    }

    public int getIOParallelism() {
        return appConfig.getInt(Option.IO_WORKERS_PER_PARTITION);
    }

    public int getIOQueueSize() {
        return appConfig.getInt(Option.IO_QUEUE_SIZE);
    }
}
