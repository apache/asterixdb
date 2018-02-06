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
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

abstract class HyracksServerProcess {
    private static final Logger LOGGER = LogManager.getLogger();

    protected Process process;
    protected File configFile = null;
    protected File logFile = null;
    protected File appHome = null;
    protected File workingDir = null;

    public void start() throws IOException {
        String[] cmd = buildCommand();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting command: " + Arrays.toString(cmd));
        }

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        if (logFile != null) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Logging to: " + logFile.getCanonicalPath());
            }
            logFile.getParentFile().mkdirs();
            try (FileWriter writer = new FileWriter(logFile, true)) {
                writer.write("---------------------\n");
            }
            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
        } else {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Logfile not set, subprocess will output to stdout");
            }
        }
        pb.directory(workingDir);
        process = pb.start();
    }

    public void stop() {
        process.destroy();
        try {
            boolean success = process.waitFor(30, TimeUnit.SECONDS);
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Killing unresponsive NC Process");
            }
            if (!success) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop(boolean forcibly) {
        if (forcibly) {
            process.destroyForcibly();
        } else {
            process.destroy();
        }
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String[] buildCommand() {
        List<String> cList = new ArrayList<String>();
        cList.add(getJavaCommand());
        addJvmArgs(cList);
        cList.add("-Dapp.home=" + appHome.getAbsolutePath());
        cList.add("-classpath");
        cList.add(getClasspath());
        cList.add(getMainClassName());
        if (configFile != null) {
            cList.add("-config-file");
            cList.add(configFile.getAbsolutePath());
        }
        addCmdLineArgs(cList);
        return cList.toArray(new String[cList.size()]);
    }

    protected void addJvmArgs(List<String> cList) {
    }

    protected void addCmdLineArgs(List<String> cList) {
    }

    protected abstract String getMainClassName();

    private final String getClasspath() {
        return System.getProperty("java.class.path");
    }

    private final String getJavaCommand() {
        return System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    }
}
