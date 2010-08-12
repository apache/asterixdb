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
package edu.uci.ics.hyracks.api.control;

import java.io.Serializable;

import org.kohsuke.args4j.Option;

public class NCConfig implements Serializable{
    @Option(name = "-cc-host", usage = "Cluster Controller host name")
    public String ccHost;

    @Option(name = "-cc-port", usage = "Cluster Controller port (default: 1099)")
    public int ccPort = 1099;

    @Option(name = "-node-id", usage = "Logical name of node controller unique within the cluster")
    public String nodeId;

    @Option(name = "-data-ip-address", usage = "IP Address to bind data listener")
    public String dataIPAddress;

    @Option(name = "-frame-size", usage = "Frame Size to use for data communication (default: 32768)")
    public int frameSize = 32768;

    @Option(name = "-dcache-client-servers", usage = "Sets the list of DCache servers in the format host1:port1,host2:port2,... (default localhost:54583)")
    public String dcacheClientServers = "localhost:54583";

    @Option(name = "-dcache-client-server-local", usage = "Sets the local DCache server, if one is available, in the format host:port (default not set)")
    public String dcacheClientServerLocal;

    @Option(name = "-dcache-client-path", usage = "Sets the path to store the files retrieved from the DCache server (default /tmp/dcache-client)")
    public String dcacheClientPath = "/tmp/dcache-client";
}