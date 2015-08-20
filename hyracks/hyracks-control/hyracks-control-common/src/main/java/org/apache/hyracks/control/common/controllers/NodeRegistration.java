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

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatSchema;

public final class NodeRegistration implements Serializable {
    private static final long serialVersionUID = 1L;

    private final InetSocketAddress ncAddress;

    private final String nodeId;

    private final NCConfig ncConfig;

    private final NetworkAddress dataPort;

    private final NetworkAddress datasetPort;

    private final String osName;

    private final String arch;

    private final String osVersion;

    private final int nProcessors;

    private final String vmName;

    private final String vmVersion;

    private final String vmVendor;

    private final String classpath;

    private final String libraryPath;

    private final String bootClasspath;

    private final List<String> inputArguments;

    private final Map<String, String> systemProperties;

    private final HeartbeatSchema hbSchema;

    public NodeRegistration(InetSocketAddress ncAddress, String nodeId, NCConfig ncConfig, NetworkAddress dataPort,
            NetworkAddress datasetPort, String osName, String arch, String osVersion, int nProcessors, String vmName,
            String vmVersion, String vmVendor, String classpath, String libraryPath, String bootClasspath,
            List<String> inputArguments, Map<String, String> systemProperties, HeartbeatSchema hbSchema) {
        this.ncAddress = ncAddress;
        this.nodeId = nodeId;
        this.ncConfig = ncConfig;
        this.dataPort = dataPort;
        this.datasetPort = datasetPort;
        this.osName = osName;
        this.arch = arch;
        this.osVersion = osVersion;
        this.nProcessors = nProcessors;
        this.vmName = vmName;
        this.vmVersion = vmVersion;
        this.vmVendor = vmVendor;
        this.classpath = classpath;
        this.libraryPath = libraryPath;
        this.bootClasspath = bootClasspath;
        this.inputArguments = inputArguments;
        this.systemProperties = systemProperties;
        this.hbSchema = hbSchema;
    }

    public InetSocketAddress getNodeControllerAddress() {
        return ncAddress;
    }

    public String getNodeId() {
        return nodeId;
    }

    public NCConfig getNCConfig() {
        return ncConfig;
    }

    public NetworkAddress getDataPort() {
        return dataPort;
    }

    public NetworkAddress getDatasetPort() {
        return datasetPort;
    }

    public String getOSName() {
        return osName;
    }

    public String getArch() {
        return arch;
    }

    public String getOSVersion() {
        return osVersion;
    }

    public int getNProcessors() {
        return nProcessors;
    }

    public HeartbeatSchema getHeartbeatSchema() {
        return hbSchema;
    }

    public String getVmName() {
        return vmName;
    }

    public String getVmVersion() {
        return vmVersion;
    }

    public String getVmVendor() {
        return vmVendor;
    }

    public String getClasspath() {
        return classpath;
    }

    public String getLibraryPath() {
        return libraryPath;
    }

    public String getBootClasspath() {
        return bootClasspath;
    }

    public List<String> getInputArguments() {
        return inputArguments;
    }

    public Map<String, String> getSystemProperties() {
        return systemProperties;
    }
}