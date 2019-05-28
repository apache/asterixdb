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

import static org.apache.hyracks.control.common.config.OptionTypes.BOOLEAN;
import static org.apache.hyracks.control.common.config.OptionTypes.LONG;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.SHORT;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;
import static org.apache.hyracks.control.common.config.OptionTypes.UNSIGNED_INTEGER;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.util.file.FileUtil;
import org.ini4j.Ini;

@SuppressWarnings("SameParameterValue")
public class CCConfig extends ControllerConfig {
    private static final long serialVersionUID = 4118822454622201176L;

    public enum Option implements IOption {
        APP_CLASS(STRING, (String) null),
        ADDRESS(STRING, InetAddress.getLoopbackAddress().getHostAddress()),
        PUBLIC_ADDRESS(STRING, ADDRESS),
        CLUSTER_LISTEN_ADDRESS(STRING, ADDRESS),
        CLUSTER_LISTEN_PORT(UNSIGNED_INTEGER, 1099),
        CLUSTER_PUBLIC_ADDRESS(STRING, PUBLIC_ADDRESS),
        CLUSTER_PUBLIC_PORT(UNSIGNED_INTEGER, CLUSTER_LISTEN_PORT),
        CLIENT_LISTEN_ADDRESS(STRING, ADDRESS),
        CLIENT_LISTEN_PORT(UNSIGNED_INTEGER, 1098),
        CLIENT_PUBLIC_ADDRESS(STRING, PUBLIC_ADDRESS),
        CLIENT_PUBLIC_PORT(UNSIGNED_INTEGER, CLIENT_LISTEN_PORT),
        CONSOLE_LISTEN_ADDRESS(STRING, ADDRESS),
        CONSOLE_LISTEN_PORT(UNSIGNED_INTEGER, 16001),
        CONSOLE_PUBLIC_ADDRESS(STRING, PUBLIC_ADDRESS),
        CONSOLE_PUBLIC_PORT(UNSIGNED_INTEGER, CONSOLE_LISTEN_PORT),
        HEARTBEAT_PERIOD(LONG, 10000L), // TODO (mblow): add time unit
        HEARTBEAT_MAX_MISSES(UNSIGNED_INTEGER, 5),
        DEAD_NODE_SWEEP_THRESHOLD(LONG, HEARTBEAT_PERIOD),
        PROFILE_DUMP_PERIOD(UNSIGNED_INTEGER, 0),
        JOB_HISTORY_SIZE(UNSIGNED_INTEGER, 10),
        RESULT_TTL(LONG, 86400000L), // TODO(mblow): add time unit
        RESULT_SWEEP_THRESHOLD(LONG, 60000L), // TODO(mblow): add time unit
        @SuppressWarnings("RedundantCast") // not redundant- false positive from IDEA
        ROOT_DIR(STRING, (Function<IApplicationConfig, String>) appConfig -> FileUtil.joinPath(appConfig.getString(ControllerConfig.Option.DEFAULT_DIR), "ClusterControllerService"), "<value of " + ControllerConfig.Option.DEFAULT_DIR.cmdline() + ">/ClusterControllerService"),
        CLUSTER_TOPOLOGY(STRING),
        JOB_QUEUE_CLASS(STRING, "org.apache.hyracks.control.cc.scheduler.FIFOJobQueue"),
        JOB_QUEUE_CAPACITY(POSITIVE_INTEGER, 4096),
        JOB_MANAGER_CLASS(STRING, "org.apache.hyracks.control.cc.job.JobManager"),
        ENFORCE_FRAME_WRITER_PROTOCOL(BOOLEAN, false),
        CORES_MULTIPLIER(POSITIVE_INTEGER, 3),
        CONTROLLER_ID(SHORT, (short) 0x0000),
        KEY_STORE_PATH(STRING),
        TRUST_STORE_PATH(STRING),
        KEY_STORE_PASSWORD(STRING);

        private final IOptionType parser;
        private Object defaultValue;
        private final String defaultValueDescription;

        <T> Option(IOptionType<T> parser) {
            this(parser, (T) null);
        }

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
            return Section.CC;
        }

        @Override
        public IOptionType type() {
            return parser;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }

        @Override
        public String description() {
            switch (this) {
                case APP_CLASS:
                    return "Application CC main class";
                case ADDRESS:
                    return "Default bind address for all services on this cluster controller";
                case PUBLIC_ADDRESS:
                    return "Default public address that other processes should use to contact this cluster controller. "
                            + "All services will advertise this address unless a service-specific public address is "
                            + "supplied.";
                case CLUSTER_LISTEN_ADDRESS:
                    return "Sets the IP Address to listen for connections from NCs";
                case CLUSTER_LISTEN_PORT:
                    return "Sets the port to listen for connections from node controllers";
                case CLUSTER_PUBLIC_ADDRESS:
                    return "Address that NCs should use to contact this CC";
                case CLUSTER_PUBLIC_PORT:
                    return "Port that NCs should use to contact this CC";
                case CLIENT_LISTEN_ADDRESS:
                    return "Sets the IP Address to listen for connections from clients";
                case CLIENT_LISTEN_PORT:
                    return "Sets the port to listen for connections from clients";
                case CLIENT_PUBLIC_ADDRESS:
                    return "The IP Address which clients should use to connect";
                case CLIENT_PUBLIC_PORT:
                    return "The port which clients should use to connect";
                case CONSOLE_LISTEN_ADDRESS:
                    return "Sets the listen address for the Cluster Controller";
                case CONSOLE_LISTEN_PORT:
                    return "Sets the http port for the Cluster Controller)";
                case CONSOLE_PUBLIC_ADDRESS:
                    return "Sets the address on which to contact the http console for the Cluster Controller";
                case CONSOLE_PUBLIC_PORT:
                    return "Sets the port on which to contact the http console for the Cluster Controller)";
                case HEARTBEAT_PERIOD:
                    return "Sets the time duration between two heartbeats from each node controller in milliseconds";
                case HEARTBEAT_MAX_MISSES:
                    return "Sets the maximum number of missed heartbeats before a node can be considered dead";
                case DEAD_NODE_SWEEP_THRESHOLD:
                    return "Sets the frequency (in milliseconds) to process nodes that can be considered dead";
                case PROFILE_DUMP_PERIOD:
                    return "Sets the time duration between two profile dumps from each node controller in "
                            + "milliseconds; 0 to disable";
                case JOB_HISTORY_SIZE:
                    return "Limits the number of historical jobs remembered by the system to the specified value";
                case RESULT_TTL:
                    return "Limits the amount of time results for asynchronous jobs should be retained by the system "
                            + "in milliseconds";
                case RESULT_SWEEP_THRESHOLD:
                    return "The duration within which an instance of the result cleanup should be invoked in "
                            + "milliseconds";
                case ROOT_DIR:
                    return "Sets the root folder used for file operations";
                case CLUSTER_TOPOLOGY:
                    return "Sets the XML file that defines the cluster topology";
                case JOB_QUEUE_CLASS:
                    return "Specify the implementation class name for the job queue";
                case JOB_QUEUE_CAPACITY:
                    return "The maximum number of jobs to queue before rejecting new jobs";
                case JOB_MANAGER_CLASS:
                    return "Specify the implementation class name for the job manager";
                case ENFORCE_FRAME_WRITER_PROTOCOL:
                    return "A flag indicating if runtime should enforce frame writer protocol and detect "
                            + "bad behaving operators";
                case CORES_MULTIPLIER:
                    return "the factor to multiply by the number of cores to determine maximum query concurrent "
                            + "execution level";
                case CONTROLLER_ID:
                    return "The 16-bit (0-65535) id of this Cluster Controller";
                case KEY_STORE_PATH:
                    return "A fully-qualified path to a key store file that will be used for secured connections";
                case TRUST_STORE_PATH:
                    return "A fully-qualified path to a trust store file that will be used for secured connections";
                case KEY_STORE_PASSWORD:
                    return "The password to the provided key store";
                default:
                    throw new IllegalStateException("NYI: " + this);
            }
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

    private List<String> appArgs = new ArrayList<>();

    public CCConfig() {
        this(new ConfigManager());
    }

    public CCConfig(ConfigManager configManager) {
        super(configManager);
        configManager.register(Option.class);
        configManager.register(ControllerConfig.Option.class);
        configManager.registerArgsListener(appArgs::addAll);
    }

    public List<String> getAppArgs() {
        return appArgs;
    }

    public String[] getAppArgsArray() {
        return appArgs.toArray(new String[appArgs.size()]);
    }

    /**
     * Returns the global config Ini file. Note: this will be null
     * if -config-file wasn't specified.
     */
    public Ini getIni() {
        return configManager.toIni(false);
    }

    // QQQ Note that clusterListenAddress is *not directly used* yet. Both
    // the cluster listener and the web server listen on "all interfaces".
    // This IP address is only used to instruct the NC on which IP to call in.
    public String getClusterListenAddress() {
        return getAppConfig().getString(Option.CLUSTER_LISTEN_ADDRESS);
    }

    public void setClusterListenAddress(String clusterListenAddress) {
        configManager.set(Option.CLUSTER_LISTEN_ADDRESS, clusterListenAddress);
    }

    public int getClusterListenPort() {
        return getAppConfig().getInt(Option.CLUSTER_LISTEN_PORT);
    }

    public void setClusterListenPort(int clusterListenPort) {
        configManager.set(Option.CLUSTER_LISTEN_PORT, clusterListenPort);
    }

    public String getClusterPublicAddress() {
        return getAppConfig().getString(Option.CLUSTER_PUBLIC_ADDRESS);
    }

    public void setClusterPublicAddress(String clusterPublicAddress) {
        configManager.set(Option.CLUSTER_PUBLIC_ADDRESS, clusterPublicAddress);
    }

    public int getClusterPublicPort() {
        return getAppConfig().getInt(Option.CLUSTER_PUBLIC_PORT);
    }

    public void setClusterPublicPort(int clusterPublicPort) {
        configManager.set(Option.CLUSTER_PUBLIC_PORT, clusterPublicPort);
    }

    public String getClientListenAddress() {
        return getAppConfig().getString(Option.CLIENT_LISTEN_ADDRESS);
    }

    public void setClientListenAddress(String clientListenAddress) {
        configManager.set(Option.CLIENT_LISTEN_ADDRESS, clientListenAddress);
    }

    public int getClientListenPort() {
        return getAppConfig().getInt(Option.CLIENT_LISTEN_PORT);
    }

    public void setClientListenPort(int clientListenPort) {
        configManager.set(Option.CLIENT_LISTEN_PORT, clientListenPort);
    }

    public String getClientPublicAddress() {
        return getAppConfig().getString(Option.CLIENT_PUBLIC_ADDRESS);
    }

    public void setClientPublicAddress(String clientPublicAddress) {
        configManager.set(Option.CLIENT_PUBLIC_ADDRESS, clientPublicAddress);
    }

    public int getClientPublicPort() {
        return getAppConfig().getInt(Option.CLIENT_PUBLIC_PORT);
    }

    public void setClientPublicPort(int clientPublicPort) {
        configManager.set(Option.CLIENT_PUBLIC_PORT, clientPublicPort);
    }

    public int getConsoleListenPort() {
        return getAppConfig().getInt(Option.CONSOLE_LISTEN_PORT);
    }

    public void setConsoleListenPort(int consoleListenPort) {
        configManager.set(Option.CONSOLE_LISTEN_PORT, consoleListenPort);
    }

    public int getConsolePublicPort() {
        return getAppConfig().getInt(Option.CONSOLE_PUBLIC_PORT);
    }

    public void setConsolePublicPort(int consolePublicPort) {
        configManager.set(Option.CONSOLE_PUBLIC_PORT, consolePublicPort);
    }

    public long getHeartbeatPeriodMillis() {
        return getAppConfig().getLong(Option.HEARTBEAT_PERIOD);
    }

    public void setHeartbeatPeriodMillis(long heartbeatPeriod) {
        configManager.set(Option.HEARTBEAT_PERIOD, heartbeatPeriod);
    }

    public int getHeartbeatMaxMisses() {
        return getAppConfig().getInt(Option.HEARTBEAT_MAX_MISSES);
    }

    public void setHeartbeatMaxMisses(int heartbeatMaxMisses) {
        configManager.set(Option.HEARTBEAT_MAX_MISSES, heartbeatMaxMisses);
    }

    public long getDeadNodeSweepThreshold() {
        return getAppConfig().getLong(Option.DEAD_NODE_SWEEP_THRESHOLD);
    }

    public void setDeadNodeSweepThreshold(long deadNodeSweepThreshold) {
        configManager.set(Option.DEAD_NODE_SWEEP_THRESHOLD, deadNodeSweepThreshold);
    }

    public int getProfileDumpPeriod() {
        return getAppConfig().getInt(Option.PROFILE_DUMP_PERIOD);
    }

    public void setProfileDumpPeriod(int profileDumpPeriod) {
        configManager.set(Option.PROFILE_DUMP_PERIOD, profileDumpPeriod);
    }

    public int getJobHistorySize() {
        return getAppConfig().getInt(Option.JOB_HISTORY_SIZE);
    }

    public void setJobHistorySize(int jobHistorySize) {
        configManager.set(Option.JOB_HISTORY_SIZE, jobHistorySize);
    }

    public long getResultTTL() {
        return getAppConfig().getLong(Option.RESULT_TTL);
    }

    public void setResultTTL(long resultTTL) {
        configManager.set(Option.RESULT_TTL, resultTTL);
    }

    public long getResultSweepThreshold() {
        return getAppConfig().getLong(Option.RESULT_SWEEP_THRESHOLD);
    }

    public void setResultSweepThreshold(long resultSweepThreshold) {
        configManager.set(Option.RESULT_SWEEP_THRESHOLD, resultSweepThreshold);
    }

    public String getRootDir() {
        return getAppConfig().getString(Option.ROOT_DIR);
    }

    public void setRootDir(String rootDir) {
        configManager.set(Option.ROOT_DIR, rootDir);
    }

    public File getClusterTopology() {
        return getAppConfig().getString(Option.CLUSTER_TOPOLOGY) == null ? null
                : new File(getAppConfig().getString(Option.CLUSTER_TOPOLOGY));
    }

    public void setClusterTopology(File clusterTopology) {
        configManager.set(Option.CLUSTER_TOPOLOGY, clusterTopology);
    }

    public String getAppClass() {
        return getAppConfig().getString(Option.APP_CLASS);
    }

    public void setAppClass(String appClass) {
        configManager.set(Option.APP_CLASS, appClass);
    }

    public String getJobQueueClass() {
        return getAppConfig().getString(Option.JOB_QUEUE_CLASS);
    }

    public void setJobQueueClass(String jobQueueClass) {
        configManager.set(Option.JOB_QUEUE_CLASS, jobQueueClass);
    }

    public String getJobManagerClass() {
        return getAppConfig().getString(Option.JOB_MANAGER_CLASS);
    }

    public void setJobManagerClass(String jobManagerClass) {
        configManager.set(Option.JOB_MANAGER_CLASS, jobManagerClass);
    }

    public int getJobQueueCapacity() {
        return getAppConfig().getInt(Option.JOB_QUEUE_CAPACITY);
    }

    public boolean getEnforceFrameWriterProtocol() {
        return getAppConfig().getBoolean(Option.ENFORCE_FRAME_WRITER_PROTOCOL);
    }

    public void setEnforceFrameWriterProtocol(boolean enforce) {
        configManager.set(Option.ENFORCE_FRAME_WRITER_PROTOCOL, enforce);
    }

    public void setCoresMultiplier(int coresMultiplier) {
        configManager.set(Option.CORES_MULTIPLIER, coresMultiplier);
    }

    public int getCoresMultiplier() {
        return getAppConfig().getInt(Option.CORES_MULTIPLIER);
    }

    public CcId getCcId() {
        return CcId.valueOf(getAppConfig().getShort(Option.CONTROLLER_ID));
    }

    public String getKeyStorePath() {
        return getAppConfig().getString(Option.KEY_STORE_PATH);
    }

    public String getKeyStorePassword() {
        return getAppConfig().getString(Option.KEY_STORE_PASSWORD);
    }

    public void setKeyStorePath(String keyStorePath) {
        configManager.set(Option.KEY_STORE_PATH, keyStorePath);
    }

    public String getTrustStorePath() {
        return getAppConfig().getString(Option.TRUST_STORE_PATH);
    }

    public void setTrustStorePath(String trustStorePath) {
        configManager.set(Option.TRUST_STORE_PATH, trustStorePath);
    }
}
