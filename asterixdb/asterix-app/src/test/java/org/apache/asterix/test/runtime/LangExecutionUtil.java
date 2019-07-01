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

package org.apache.asterix.test.runtime;

import static org.apache.asterix.test.runtime.ExecutionTestUtil.integrationUtil;
import static org.apache.hyracks.util.ThreadDumpUtil.takeDumpJSONString;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.asterix.app.external.ExternalUDFLibrarian;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.util.ThreadDumpUtil;

/**
 * Utils for running SQL++ or AQL runtime tests.
 */
public class LangExecutionUtil {

    private static final String PATH_ACTUAL = "target" + File.separator + "rttest" + File.separator;
    private static final String PATH_BASE =
            StringUtils.join(new String[] { "src", "test", "resources", "runtimets" }, File.separator);

    private static final boolean cleanupOnStart = true;
    private static final boolean cleanupOnStop = true;
    private static final List<String> badTestCases = new ArrayList<>();
    private static TestExecutor testExecutor;

    private static ExternalUDFLibrarian librarian;
    private static final int repeat = Integer.getInteger("test.repeat", 1);
    private static boolean checkStorageDistribution = true;

    public static void setUp(String configFile, TestExecutor executor) throws Exception {
        setUp(configFile, executor, false);
    }

    public static void setUp(String configFile, TestExecutor executor, boolean startHdfs) throws Exception {
        testExecutor = executor;
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        ExecutionTestUtil.setUp(cleanupOnStart, configFile, integrationUtil, startHdfs, null);
        librarian = new ExternalUDFLibrarian();
        testExecutor.setLibrarian(librarian);
        if (repeat != 1) {
            System.out.println("FYI: each test will be run " + repeat + " times.");
        }
    }

    public static void tearDown() throws Exception {
        try {
            // Check whether there are leaked open run file handles.
            checkOpenRunFileLeaks();
            // Check whether there are leaked threads.
            checkThreadLeaks();
        } finally {
            ExecutionTestUtil.tearDown(cleanupOnStop);
            integrationUtil.removeTestStorageFiles();
            if (!badTestCases.isEmpty()) {
                System.out.println("The following test cases left some data");
                for (String testCase : badTestCases) {
                    System.out.println(testCase);
                }
            }
        }
    }

    public static Collection<Object[]> tests(String onlyFilePath, String suiteFilePath) throws Exception {
        Collection<Object[]> testArgs = buildTestsInXml(onlyFilePath);
        if (testArgs.size() == 0) {
            testArgs = buildTestsInXml(suiteFilePath);
        }
        return testArgs;
    }

    protected static Collection<Object[]> buildTestsInXml(String xmlfile) throws Exception {
        Collection<Object[]> testArgs = new ArrayList<>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE), xmlfile)) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    public static void test(TestCaseContext tcCtx) throws Exception {
        test(testExecutor, tcCtx);
    }

    public static void test(TestExecutor testExecutor, TestCaseContext tcCtx) throws Exception {
        int repeat = LangExecutionUtil.repeat * tcCtx.getRepeat();
        try {
            for (int i = 1; i <= repeat; i++) {
                if (repeat > 1) {
                    System.err.print("[" + i + "/" + repeat + "] ");
                }
                testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false, ExecutionTestUtil.FailedGroup);

                try {
                    if (checkStorageDistribution) {
                        checkStorageFiles();
                    }
                } finally {
                    testExecutor.cleanup(tcCtx.toString(), badTestCases);
                }
            }
        } finally {
            System.err.flush();
        }
    }

    // Checks whether data files are uniformly distributed among io devices.
    private static void checkStorageFiles() throws Exception {
        NodeControllerService[] ncs = integrationUtil.ncs;
        // Checks that dataset files are uniformly distributed across each io device.
        for (NodeControllerService nc : ncs) {
            checkNcStore(nc);
        }
    }

    // For each NC, check whether data files are uniformly distributed among io devices.
    private static void checkNcStore(NodeControllerService nc) throws Exception {
        List<IODeviceHandle> ioDevices = nc.getIoManager().getIODevices();
        int expectedPartitionNum = -1;
        for (IODeviceHandle ioDevice : ioDevices) {
            File[] dataDirs = ioDevice.getMount().listFiles();
            for (File dataDir : dataDirs) {
                String dirName = dataDir.getName();
                if (!dirName.equals(StorageConstants.STORAGE_ROOT_DIR_NAME)) {
                    // Skips non-storage directories.
                    continue;
                }
                int numPartitions = getNumResidentPartitions(dataDir.listFiles());
                if (expectedPartitionNum < 0) {
                    // Sets the expected number of partitions to the number of partitions on the first io device.
                    expectedPartitionNum = numPartitions;
                } else {
                    // Checks whether the number of partitions of the current io device is expected.
                    if (expectedPartitionNum != numPartitions) {
                        throw new Exception("Non-uniform data distribution on io devices: " + dataDir.getAbsolutePath()
                                + " number of partitions: " + numPartitions + " expected number of partitions: "
                                + expectedPartitionNum);
                    }
                }
            }
        }
    }

    // Gets the number of partitions on each io device.
    private static int getNumResidentPartitions(File[] partitions) {
        int num = 0;
        for (File partition : partitions) {
            File[] dataverses = partition.listFiles();
            for (File dv : dataverses) {
                String dvName = dv.getName();
                // If a partition only contains the Metadata dataverse, it's not counted.
                if (!dvName.equals("Metadata")) {
                    num++;
                    break;
                }
            }
        }
        return num;
    }

    public static void checkThreadLeaks() throws IOException {
        String threadDump = ThreadDumpUtil.takeDumpJSONString();
        // Currently we only do sanity check for threads used in the execution engine.
        // Later we should check if there are leaked storage threads as well.
        if (threadDump.contains("Operator") || threadDump.contains("SuperActivity")
                || threadDump.contains("PipelinedPartition")) {
            System.out.print(threadDump);
            throw new AssertionError("There are leaked threads in the execution engine.");
        }
    }

    public static void checkOpenRunFileLeaks() throws IOException {
        if (SystemUtils.IS_OS_WINDOWS) {
            return;
        }
        // Only run the check on Linux and MacOS.
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String processName = runtimeMXBean.getName();
        String processId = processName.split("@")[0];

        // Checks whether there are leaked run files from operators.
        Process process =
                Runtime.getRuntime().exec(new String[] { "bash", "-c", "lsof -p " + processId + "|grep waf|wc -l" });
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            int runFileCount = Integer.parseInt(reader.readLine().trim());
            if (runFileCount != 0) {
                System.out.print(takeDumpJSONString());
                outputLeakedOpenFiles(processId);
                throw new AssertionError("There are " + runFileCount + " leaked run files.");
            }
        }
    }

    public static void setCheckStorageDistribution(boolean checkStorageDistribution) {
        LangExecutionUtil.checkStorageDistribution = checkStorageDistribution;
    }

    private static void outputLeakedOpenFiles(String processId) throws IOException {
        Process process =
                Runtime.getRuntime().exec(new String[] { "bash", "-c", "lsof -p " + processId + "|grep waf" });
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.err.println(line);
            }
        }
    }
}
