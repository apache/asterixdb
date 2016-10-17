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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.apache.hyracks.api.application.IApplicationConfig;
import org.apache.hyracks.control.common.application.IniApplicationConfig;
import org.ini4j.Ini;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StopOptionHandler;

public class CCConfig {
    @Option(name = "-address", usage = "IP Address for CC (default: localhost)", required = false)
    public String ipAddress = InetAddress.getLoopbackAddress().getHostAddress();

    @Option(name = "-client-net-ip-address",
            usage = "Sets the IP Address to listen for connections from clients (default: same as -address)",
            required = false)
    public String clientNetIpAddress;

    @Option(name = "-client-net-port", usage = "Sets the port to listen for connections from clients (default 1098)")
    public int clientNetPort = 1098;

    // QQQ Note that clusterNetIpAddress is *not directly used* yet. Both
    // the cluster listener and the web server listen on "all interfaces".
    // This IP address is only used to instruct the NC on which IP to call in.
    @Option(name = "-cluster-net-ip-address",
            usage = "Sets the IP Address to listen for connections from NCs (default: same as -address)",
            required = false)
    public String clusterNetIpAddress;

    @Option(name = "-cluster-net-port",
            usage = "Sets the port to listen for connections from node controllers (default 1099)")
    public int clusterNetPort = 1099;

    @Option(name = "-http-port", usage = "Sets the http port for the Cluster Controller (default: 16001)")
    public int httpPort = 16001;

    @Option(name = "-heartbeat-period",
            usage = "Sets the time duration between two heartbeats from each node controller in milliseconds" +
                    " (default: 10000)")
    public int heartbeatPeriod = 10000;

    @Option(name = "-max-heartbeat-lapse-periods",
            usage = "Sets the maximum number of missed heartbeats before a node is marked as dead (default: 5)")
    public int maxHeartbeatLapsePeriods = 5;

    @Option(name = "-profile-dump-period", usage = "Sets the time duration between two profile dumps from each node " +
            "controller in milliseconds. 0 to disable. (default: 0)")
    public int profileDumpPeriod = 0;

    @Option(name = "-default-max-job-attempts", usage = "Sets the default number of job attempts allowed if not " +
            "specified in the job specification. (default: 5)")
    public int defaultMaxJobAttempts = 5;

    @Option(name = "-job-history-size", usage = "Limits the number of historical jobs remembered by the system to " +
            "the specified value. (default: 10)")
    public int jobHistorySize = 10;

    @Option(name = "-result-time-to-live", usage = "Limits the amount of time results for asynchronous jobs should " +
            "be retained by the system in milliseconds. (default: 24 hours)")
    public long resultTTL = 86400000;

    @Option(name = "-result-sweep-threshold", usage = "The duration within which an instance of the result cleanup " +
            "should be invoked in milliseconds. (default: 1 minute)")
    public long resultSweepThreshold = 60000;

    @Option(name = "-cc-root",
            usage = "Sets the root folder used for file operations. (default: ClusterControllerService)")
    public String ccRoot = "ClusterControllerService";

    @Option(name = "-cluster-topology", required = false,
            usage = "Sets the XML file that defines the cluster topology. (default: null)")
    public File clusterTopologyDefinition = null;

    @Option(name = "-app-cc-main-class", required = false, usage = "Application CC Main Class")
    public String appCCMainClass = null;

    @Option(name = "-config-file",
            usage = "Specify path to master configuration file (default: none)", required = false)
    public String configFile = null;

    @Argument
    @Option(name = "--", handler = StopOptionHandler.class)
    public List<String> appArgs;

    private Ini ini = null;

    private void loadINIFile() throws IOException {
        // This method simply maps from the ini parameters to the CCConfig's fields.
        // It does not apply defaults or any logic.
        ini = IniUtils.loadINIFile(configFile);

        ipAddress = IniUtils.getString(ini, "cc", "address", ipAddress);
        clientNetIpAddress = IniUtils.getString(ini, "cc", "client.address", clientNetIpAddress);
        clientNetPort = IniUtils.getInt(ini, "cc", "client.port", clientNetPort);
        clusterNetIpAddress = IniUtils.getString(ini, "cc", "cluster.address", clusterNetIpAddress);
        clusterNetPort = IniUtils.getInt(ini, "cc", "cluster.port", clusterNetPort);
        httpPort = IniUtils.getInt(ini, "cc", "http.port", httpPort);
        heartbeatPeriod = IniUtils.getInt(ini, "cc", "heartbeat.period", heartbeatPeriod);
        maxHeartbeatLapsePeriods = IniUtils.getInt(ini, "cc", "heartbeat.maxlapse", maxHeartbeatLapsePeriods);
        profileDumpPeriod = IniUtils.getInt(ini, "cc", "profiledump.period", profileDumpPeriod);
        defaultMaxJobAttempts = IniUtils.getInt(ini, "cc", "job.defaultattempts", defaultMaxJobAttempts);
        jobHistorySize = IniUtils.getInt(ini, "cc", "job.historysize", jobHistorySize);
        resultTTL = IniUtils.getLong(ini, "cc", "results.ttl", resultTTL);
        resultSweepThreshold = IniUtils.getLong(ini, "cc", "results.sweepthreshold", resultSweepThreshold);
        ccRoot = IniUtils.getString(ini, "cc", "rootfolder", ccRoot);
        // QQQ clusterTopologyDefinition is a "File"; should support verifying that the file
        // exists, as @Option likely does
        appCCMainClass = IniUtils.getString(ini, "cc", "app.class", appCCMainClass);
    }

    /**
     * Once all @Option fields have been loaded from command-line or otherwise
     * specified programmatically, call this method to:
     * 1. Load options from a config file (as specified by -config-file)
     * 2. Set default values for certain derived values, such as setting
     *    clusterNetIpAddress to ipAddress
     */
    public void loadConfigAndApplyDefaults() throws IOException {
        if (configFile != null) {
            loadINIFile();
            // QQQ This way of passing overridden/defaulted values back into
            // the ini feels clunky, and it's clearly incomplete
            ini.add("cc", "cluster.address", clusterNetIpAddress);
            ini.add("cc", "client.address", clientNetIpAddress);
        }

        // "address" is the default for all IP addresses
        clusterNetIpAddress = clusterNetIpAddress == null ? ipAddress : clusterNetIpAddress;
        clientNetIpAddress = clientNetIpAddress == null ? ipAddress : clientNetIpAddress;
    }

    /**
     * Returns the global config Ini file. Note: this will be null
     * if -config-file wasn't specified.
     */
    public Ini getIni() {
        return ini;
    }

    /**
     * @return An IApplicationConfig representing this NCConfig.
     * Note: Currently this only includes the values from the configuration
     * file, not anything specified on the command-line. QQQ
     */
    public IApplicationConfig getAppConfig() {
        return new IniApplicationConfig(ini);
    }
}
