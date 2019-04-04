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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

abstract class HyracksServerProcess {
    private static final Logger LOGGER = LogManager.getLogger();

    protected final String processName;
    protected Process process;
    protected Thread pipeThread;
    protected File configFile = null;
    protected File logFile = null;
    protected File appHome = null;
    protected File workingDir = null;
    protected List<String> args = new ArrayList<>();

    protected HyracksServerProcess(String processName) {
        this.processName = processName;
    }

    public void start() throws IOException {

        String[] cmd = buildCommand();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting command: " + Arrays.toString(cmd));
        }

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        pb.directory(workingDir);
        if (logFile != null) {
            LOGGER.info("Logging to: " + logFile.getCanonicalPath());
            logFile.getParentFile().mkdirs();
            try (FileWriter writer = new FileWriter(logFile, true)) {
                writer.write("---------------------\n");
            }
            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile));
            process = pb.start();
        } else {
            pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
            process = pb.start();
            pipeThread = new Thread(() -> {
                try (BufferedReader reader =
                        new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println(processName + ": " + line);
                    }
                } catch (IOException e) {
                    LOGGER.debug("exception reading process pipe", e);
                }
            });
            pipeThread.start();
        }
    }

    public void stop() {
        process.destroy();
        try {
            boolean success = process.waitFor(30, TimeUnit.SECONDS);
            if (!success) {
                LOGGER.warn("Killing unresponsive NC Process");
                process.destroyForcibly();
            }
            if (pipeThread != null) {
                pipeThread.interrupt();
                pipeThread.join();
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
        return cList.toArray(new String[0]);
    }

    protected void addJvmArgs(List<String> cList) {
    }

    protected void addCmdLineArgs(List<String> cList) {
    }

    public void addArg(String arg) {
        args.add(arg);
    }

    protected abstract String getMainClassName();

    private final String getClasspath() {
        return System.getProperty("java.class.path");
    }

    private final String getJavaCommand() {
        return System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    }
}
