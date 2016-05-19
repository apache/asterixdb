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
package org.apache.hyracks.control.nc.service;

import org.apache.hyracks.control.common.controllers.IniUtils;
import org.ini4j.Ini;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Command-line arguments for NC Service.
 */
public class NCServiceConfig {

    /**
     * Normally one should only pass this argument. Other arguments are for debugging and test purposes.
     * If an option is specified both in the config file and on the command line, the config file
     * version will take precedence.
     */
    @Option(name = "-config-file", usage = "Local NC configuration file (default: none)", required = false)
    public String configFile = null;

    @Option(name = "-address", usage = "Address to listen on for connections from CC (default: localhost)", required = false)
    public String address = InetAddress.getLoopbackAddress().getHostAddress();

    @Option(name = "-port", usage = "Port to listen on for connections from CC (default: 9090)", required = false)
    public int port = 9090;

    @Option(name = "-command", usage = "NC command to run (default: 'hyracksnc' on PATH)", required = false)
    public String command = "hyracksnc";

    private Ini ini = null;

    /**
     * This method simply maps from the ini parameters to the NCServiceConfig's fields.
     * It does not apply defaults or any logic.
     */
    private void loadINIFile() throws IOException {
        ini = IniUtils.loadINIFile(configFile);
        address = IniUtils.getString(ini, "ncservice", "address", address);
        port = IniUtils.getInt(ini, "ncservice", "port", port);
    }

    /**
     * Once all @Option fields have been loaded from command-line or otherwise
     * specified programmatically, call this method to:
     * 1. Load options from a config file (as specified by -config-file)
     * 2. Set default values for certain derived values
     */
    public void loadConfigAndApplyDefaults() throws IOException {
        if (configFile != null) {
            loadINIFile();
        }
        // No defaults necessary beyond the static ones for this config
    }
}
