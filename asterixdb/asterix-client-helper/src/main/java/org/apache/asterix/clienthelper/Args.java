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
package org.apache.asterix.clienthelper;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

@SuppressWarnings("FieldCanBeLocal")
public class Args {

    @Option(name = "-clusteraddress", metaVar = "<address>", usage = "Hostname or IP Address of the cluster")
    protected String clusterAddress = InetAddress.getLoopbackAddress().getHostAddress();

    @Option(name = "-clusterport", metaVar = "<port>", usage = "Port of the cluster to connect to")
    protected int clusterPort = 19002;

    @Option(name = "-clusterstatepath", metaVar = "<path>", hidden = true, usage = "Path on host:port to check for cluster readiness")
    protected String clusterStatePath = "admin/cluster";

    @Option(name = "-shutdownpath", metaVar = "<path>", hidden = true, usage = "Path on host:port to invoke to initiate shutdown")
    protected String shutdownPath = "admin/shutdown";

    @Option(name = "-timeout", metaVar = "<secs>", usage = "Timeout for wait commands in seconds")
    protected int timeoutSecs = 0;

    @Option(name = "-quiet", aliases = "-q", usage = "Suppress all normal output")
    protected boolean quiet;

    @Argument
    protected List<String> arguments = new ArrayList<>();

    public String getClusterAddress() {
        return clusterAddress;
    }

    public int getClusterPort() {
        return clusterPort;
    }

    public int getTimeoutSecs() {
        return timeoutSecs;
    }

    public boolean isQuiet() {
        return quiet;
    }

    public String getClusterStatePath() {
        return clusterStatePath;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public String getShutdownPath() {
        return shutdownPath;
    }
}
