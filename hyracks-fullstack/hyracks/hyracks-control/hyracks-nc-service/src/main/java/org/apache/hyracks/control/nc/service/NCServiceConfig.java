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

import java.io.File;
import java.io.IOException;

import org.apache.hyracks.control.common.config.ConfigUtils;
import org.ini4j.Ini;
import org.kohsuke.args4j.Option;

/**
 * Command-line arguments for NC Service.
 */
public class NCServiceConfig {

    /**
     * Normally one should only pass this argument. Other arguments are for debugging and test purposes.
     * If an option is specified both in the config file and on the command line, the config file
     * version will take precedence.
     */
    @Option(name = "-config-file", required = false, usage = "Local NC configuration file (default: none)")
    public String configFile = null;

    @Option(name = "-address", required = false, usage = "Address to listen on for connections from CC (default: all addresses)")
    public String address = null;

    @Option(name = "-port", required = false, usage = "Port to listen on for connections from CC (default: 9090)")
    public int port = 9090;

    @Option(name = "-logdir", required = false, usage = "Directory to log NC output ('-' for stdout of NC service; default: $app.home/logs)")
    public String logdir = null;

    private Ini ini = null;

    /**
     * This method simply maps from the ini parameters to the NCServiceConfig's fields.
     * It does not apply defaults or any logic.
     */
    private void loadINIFile() throws IOException {
        ini = ConfigUtils.loadINIFile(configFile);
        address = ConfigUtils.getString(ini, "ncservice", "address", address);
        port = ConfigUtils.getInt(ini, "ncservice", "port", port);
        logdir = ConfigUtils.getString(ini, "ncservice", "logdir", logdir);
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
        if (logdir == null) {
            logdir = System.getProperty("app.home", System.getProperty("user.home")) + File.separator + "logs";
        }
    }
}
