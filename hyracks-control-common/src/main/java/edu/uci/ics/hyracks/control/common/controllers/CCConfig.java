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
package edu.uci.ics.hyracks.control.common.controllers;

import java.util.List;

import org.kohsuke.args4j.Option;

public class CCConfig {
    @Option(name = "-port", usage = "Sets the port to listen for connections from node controllers (default 1099)")
    public int port = 1099;

    @Option(name = "-http-port", usage = "Sets the http port for the Cluster Controller")
    public int httpPort;

    @Option(name = "-heartbeat-period", usage = "Sets the time duration between two heartbeats from each node controller in milliseconds (default: 10000)")
    public int heartbeatPeriod = 10000;

    @Option(name = "-max-heartbeat-lapse-periods", usage = "Sets the maximum number of missed heartbeats before a node is marked as dead (default: 5)")
    public int maxHeartbeatLapsePeriods = 5;

    @Option(name = "-profile-dump-period", usage = "Sets the time duration between two profile dumps from each node controller in milliseconds. 0 to disable. (default: 0)")
    public int profileDumpPeriod = 0;

    @Option(name = "-default-max-job-attempts", usage = "Sets the default number of job attempts allowed if not specified in the job specification. (default: 5)")
    public int defaultMaxJobAttempts = 5;

    public void toCommandLine(List<String> cList) {
        cList.add("-port");
        cList.add(String.valueOf(port));
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
    }
}