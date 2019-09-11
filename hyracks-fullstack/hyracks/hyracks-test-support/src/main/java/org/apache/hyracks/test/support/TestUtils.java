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
package org.apache.hyracks.test.support;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.control.nc.io.DefaultDeviceResolver;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;

public class TestUtils {

    public static final IWarningCollector NOOP_WARNING_COLLECTOR = new IWarningCollector() {
        @Override
        public void warn(Warning warning) {
            // no-op
        }

        @Override
        public boolean shouldWarn() {
            return false;
        }

        @Override
        public long getTotalWarningsCount() {
            return 0;
        }
    };

    public static IHyracksTaskContext create(int frameSize) {
        IOManager ioManager = null;
        try {
            ioManager = createIoManager();
            return create(frameSize, ioManager);
        } catch (Exception e) {
            if (ioManager != null) {
                CleanupUtils.close(ioManager, e);
            }
            throw new RuntimeException(e);
        }
    }

    public static IHyracksTaskContext create(int frameSize, IOManager ioManager) {
        try {
            INCServiceContext serviceCtx = new TestNCServiceContext(ioManager, null);
            TestJobletContext jobletCtx = new TestJobletContext(frameSize, serviceCtx, new JobId(0));
            TaskAttemptId tid = new TaskAttemptId(new TaskId(new ActivityId(new OperatorDescriptorId(0), 0), 0), 0);
            IHyracksTaskContext taskCtx = new TestTaskContext(jobletCtx, tid);
            return taskCtx;
        } catch (HyracksException e) {
            throw new RuntimeException(e);
        }
    }

    private static IOManager createIoManager() throws HyracksException {
        List<IODeviceHandle> devices = new ArrayList<>();
        devices.add(new IODeviceHandle(new File(System.getProperty("java.io.tmpdir")), "."));
        return new IOManager(devices, new DefaultDeviceResolver(), 2, 10);
    }

    public static void compareWithResult(File expectedFile, File actualFile) throws Exception {
        String lineExpected, lineActual;
        int num = 1;
        try (BufferedReader readerExpected = new BufferedReader(new FileReader(expectedFile));
                BufferedReader readerActual = new BufferedReader(new FileReader(actualFile))) {
            while ((lineExpected = readerExpected.readLine()) != null) {
                lineActual = readerActual.readLine();
                if (lineActual == null) {
                    throw new Exception("Actual result changed at line " + num + ":\n< " + lineExpected + "\n> ");
                }
                if (!equalStrings(lineExpected, lineActual)) {
                    throw new Exception(
                            "Result for changed at line " + num + ":\n< " + lineExpected + "\n> " + lineActual);
                }
                ++num;
            }
            lineActual = readerActual.readLine();
            if (lineActual != null) {
                throw new Exception("Actual result changed at line " + num + ":\n< \n> " + lineActual);
            }
        }
    }

    private static boolean equalStrings(String s1, String s2) {
        String[] rowsOne = s1.split("\n");
        String[] rowsTwo = s2.split("\n");

        if (rowsOne.length != rowsTwo.length) {
            return false;
        }

        for (int i = 0; i < rowsOne.length; i++) {
            String row1 = rowsOne[i];
            String row2 = rowsTwo[i];

            if (row1.equals(row2)) {
                continue;
            }

            String[] fields1 = row1.split(",");
            String[] fields2 = row2.split(",");

            for (int j = 0; j < fields1.length; j++) {
                if (fields1[j].equals(fields2[j])) {
                    continue;
                } else if (fields1[j].indexOf('.') < 0) {
                    return false;
                } else {
                    fields1[j] = fields1[j].split("=")[1];
                    fields2[j] = fields2[j].split("=")[1];
                    Double double1 = Double.parseDouble(fields1[j]);
                    Double double2 = Double.parseDouble(fields2[j]);
                    float float1 = (float) double1.doubleValue();
                    float float2 = (float) double2.doubleValue();

                    if (Math.abs(float1 - float2) == 0) {
                        continue;
                    } else {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static Map<String, NodeControllerInfo> generateNodeControllerInfo(int numberOfNodes, String ncNamePrefix,
            String addressPrefix, int netPort, int dataPort, int messagingPort) {
        Map<String, NodeControllerInfo> ncNameToNcInfos = new HashMap<>();
        for (int i = 1; i <= numberOfNodes; i++) {
            String ncId = ncNamePrefix + i;
            String ncAddress = addressPrefix + i;
            ncNameToNcInfos.put(ncId,
                    new NodeControllerInfo(ncId, NodeStatus.ACTIVE, new NetworkAddress(ncAddress, netPort),
                            new NetworkAddress(ncAddress, dataPort), new NetworkAddress(ncAddress, messagingPort), 2));
        }
        return ncNameToNcInfos;
    }

    public static void redirectLoggingToConsole() {
        final LoggerContext context = LoggerContext.getContext(false);
        final Configuration config = context.getConfiguration();

        Appender appender = config.getAppender("Console");
        if (appender == null) {
            Optional<Appender> result =
                    config.getAppenders().values().stream().filter(a -> a instanceof ConsoleAppender).findFirst();
            if (!result.isPresent()) {
                System.err.println(
                        "ERROR: cannot find appender named 'Console'; unable to find alternate ConsoleAppender!");
            } else {
                appender = result.get();
                System.err.println("ERROR: cannot find appender named 'Console'; using alternate ConsoleAppender named "
                        + appender.getName());
            }
        }
        if (appender != null) {
            config.getRootLogger().addAppender(appender, null, null);
            context.updateLoggers();
        }
    }
}
