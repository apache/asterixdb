/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.common.controllers;

import java.io.File;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StopOptionHandler;

public class CCConfig {
    @Option(name = "-client-net-ip-address", usage = "Sets the IP Address to listen for connections from clients", required = true)
    public String clientNetIpAddress;

    @Option(name = "-client-net-port", usage = "Sets the port to listen for connections from clients (default 1098)")
    public int clientNetPort = 1098;

    @Option(name = "-cluster-net-ip-address", usage = "Sets the IP Address to listen for connections from", required = true)
    public String clusterNetIpAddress;

    @Option(name = "-cluster-net-port", usage = "Sets the port to listen for connections from node controllers (default 1099)")
    public int clusterNetPort = 1099;

    @Option(name = "-http-port", usage = "Sets the http port for the Cluster Controller (default: 19001)")
    public int httpPort = 16001;

    @Option(name = "-heartbeat-period", usage = "Sets the time duration between two heartbeats from each node controller in milliseconds (default: 10000)")
    public int heartbeatPeriod = 10000;

    @Option(name = "-max-heartbeat-lapse-periods", usage = "Sets the maximum number of missed heartbeats before a node is marked as dead (default: 5)")
    public int maxHeartbeatLapsePeriods = 5;

    @Option(name = "-profile-dump-period", usage = "Sets the time duration between two profile dumps from each node controller in milliseconds. 0 to disable. (default: 0)")
    public int profileDumpPeriod = 0;

    @Option(name = "-default-max-job-attempts", usage = "Sets the default number of job attempts allowed if not specified in the job specification. (default: 5)")
    public int defaultMaxJobAttempts = 5;

    @Option(name = "-job-history-size", usage = "Limits the number of historical jobs remembered by the system to the specified value. (default: 10)")
    public int jobHistorySize = 10;

    @Option(name = "-result-time-to-live", usage = "Limits the amount of time results for asynchronous jobs should be retained by the system in milliseconds. (default: 24 hours)")
    public long resultTTL = 86400000;

    @Option(name = "-result-sweep-threshold", usage = "The duration within which an instance of the result cleanup should be invoked in milliseconds. (default: 1 minute)")
    public long resultSweepThreshold = 60000;

    @Option(name = "-cc-root", usage = "Sets the root folder used for file operations. (default: ClusterControllerService)")
    public String ccRoot = "ClusterControllerService";

    @Option(name = "-cluster-topology", required = false, usage = "Sets the XML file that defines the cluster topology. (default: null)")
    public File clusterTopologyDefinition = null;

    @Option(name = "-app-cc-main-class", required = false, usage = "Application CC Main Class")
    public String appCCMainClass = null;

    @Argument
    @Option(name = "--", handler = StopOptionHandler.class)
    public List<String> appArgs;

    public void toCommandLine(List<String> cList) {
        cList.add("-client-net-ip-address");
        cList.add(clientNetIpAddress);
        cList.add("-client-net-port");
        cList.add(String.valueOf(clientNetPort));
        cList.add("-cluster-net-ip-address");
        cList.add(clusterNetIpAddress);
        cList.add("-cluster-net-port");
        cList.add(String.valueOf(clusterNetPort));
        cList.add("-http-port");
        cList.add(String.valueOf(httpPort));
        cList.add("-heartbeat-period");
        cList.add(String.valueOf(heartbeatPeriod));
        cList.add("-max-heartbeat-lapse-periods");
        cList.add(String.valueOf(maxHeartbeatLapsePeriods));
        cList.add("-profile-dump-period");
        cList.add(String.valueOf(profileDumpPeriod));
        cList.add("-default-max-job-attempts");
        cList.add(String.valueOf(defaultMaxJobAttempts));
        cList.add("-job-history-size");
        cList.add(String.valueOf(jobHistorySize));
        cList.add("-result-time-to-live");
        cList.add(String.valueOf(resultTTL));
        cList.add("-result-sweep-threshold");
        cList.add(String.valueOf(resultSweepThreshold));
        cList.add("-cc-root");
        cList.add(ccRoot);
        if (clusterTopologyDefinition != null) {
            cList.add("-cluster-topology");
            cList.add(clusterTopologyDefinition.getAbsolutePath());
        }
        if (appCCMainClass != null) {
            cList.add("-app-cc-main-class");
            cList.add(appCCMainClass);
        }
        if (appArgs != null && !appArgs.isEmpty()) {
            cList.add("--");
            for (String appArg : appArgs) {
                cList.add(appArg);
            }
        }
    }
}