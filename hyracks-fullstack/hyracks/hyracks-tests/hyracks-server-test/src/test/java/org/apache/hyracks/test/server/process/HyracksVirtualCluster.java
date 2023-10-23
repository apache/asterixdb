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

package org.apache.hyracks.test.server.process;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Starts a local hyracks-based cluster (NC and CC child processes).
 */
public class HyracksVirtualCluster {
    private final File appHome;
    private final File workingDir;
    private List<HyracksNCServiceProcess> ncProcs = new ArrayList<>(3);
    private HyracksCCProcess ccProc = null;

    /**
     * Construct a Hyracks-based cluster.
     *
     * @param appHome
     *            - path to the installation root of the Hyracks application.
     *            At least bin/hyracksnc (or the equivalent NC script for
     *            the application) must exist in this directory.
     * @param workingDir
     *            - directory to use as CWD for all child processes. May
     *            be null, in which case the CWD of the invoking process is used.
     */
    public HyracksVirtualCluster(File appHome, File workingDir) {
        this.appHome = appHome;
        this.workingDir = workingDir;
    }

    /**
     * Creates and starts an NCService.
     *
     * @param configFile
     *            - full path to an ncservice.conf. May be null to accept all defaults.
     * @throws IOException
     *             - if there are errors starting the process.
     */
    public HyracksNCServiceProcess addNCService(File configFile, File logFile) throws IOException {
        HyracksNCServiceProcess proc = new HyracksNCServiceProcess(configFile, logFile, appHome, workingDir);
        proc.start();
        ncProcs.add(proc);
        return proc;
    }

    /**
     * Starts the CC, initializing the cluster. Expects that any NCs referenced
     * in the cluster configuration have already been started with addNCService().
     *
     * @param ccConfigFile
     *            - full path to a cluster conf file. May be null to accept all
     *            defaults, although this is seldom useful since there are no NCs.
     * @throws IOException
     *             - if there are errors starting the process.
     */
    public HyracksCCProcess start(File ccConfigFile, File logFile) throws IOException {
        ccProc = new HyracksCCProcess(ccConfigFile, logFile, appHome, workingDir);
        ccProc.start();
        return ccProc;
    }

    /**
     * Stops all processes in the cluster.
     * QQQ Someday this should probably do a graceful stop of NCs rather than
     * killing the NCService.
     */
    public void stop() {
        ccProc.stop();
        for (HyracksNCServiceProcess proc : ncProcs) {
            proc.stop();
        }
    }
}
