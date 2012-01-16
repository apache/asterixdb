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

import java.io.Serializable;
import java.util.List;

import org.kohsuke.args4j.Option;

public class NCConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @Option(name = "-cc-host", usage = "Cluster Controller host name", required = true)
    public String ccHost;

    @Option(name = "-cc-port", usage = "Cluster Controller port (default: 1099)")
    public int ccPort = 1099;

    @Option(name = "-cluster-net-ip-address", usage = "IP Address to bind cluster listener", required = true)
    public String clusterNetIPAddress;

    @Option(name = "-node-id", usage = "Logical name of node controller unique within the cluster", required = true)
    public String nodeId;

    @Option(name = "-data-ip-address", usage = "IP Address to bind data listener", required = true)
    public String dataIPAddress;

    @Option(name = "-frame-size", usage = "Frame Size to use for data communication (default: 32768)")
    public int frameSize = 32768;

    @Option(name = "-iodevices", usage = "Comma separated list of IO Device mount points (default: One device in default temp folder)", required = false)
    public String ioDevices = System.getProperty("java.io.tmpdir");

    @Option(name = "-dcache-client-servers", usage = "Sets the list of DCache servers in the format host1:port1,host2:port2,... (default localhost:54583)")
    public String dcacheClientServers = "localhost:54583";

    @Option(name = "-dcache-client-server-local", usage = "Sets the local DCache server, if one is available, in the format host:port (default not set)")
    public String dcacheClientServerLocal;

    @Option(name = "-dcache-client-path", usage = "Sets the path to store the files retrieved from the DCache server (default /tmp/dcache-client)")
    public String dcacheClientPath = "/tmp/dcache-client";

    public void toCommandLine(List<String> cList) {
        cList.add("-cc-host");
        cList.add(ccHost);
        cList.add("-cc-port");
        cList.add(String.valueOf(ccPort));
        cList.add("-cluster-net-ip-address");
        cList.add(clusterNetIPAddress);
        cList.add("-node-id");
        cList.add(nodeId);
        cList.add("-data-ip-address");
        cList.add(dataIPAddress);
        cList.add("-frame-size");
        cList.add(String.valueOf(frameSize));
        cList.add("-iodevices");
        cList.add(ioDevices);
        cList.add("-dcache-client-servers");
        cList.add(dcacheClientServers);
        if (dcacheClientServerLocal != null) {
            cList.add("-dcache-client-server-local");
            cList.add(dcacheClientServerLocal);
        }
        cList.add("-dcache-client-path");
        cList.add(dcacheClientPath);
    }
}