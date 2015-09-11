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
package org.apache.hyracks.tests.integration;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.IHyracksDatasetReader;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public abstract class AbstractIntegrationTest {
    private static final Logger LOGGER = Logger.getLogger(AbstractIntegrationTest.class.getName());

    public static final String NC1_ID = "nc1";
    public static final String NC2_ID = "nc2";

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static NodeControllerService nc2;
    private static IHyracksClientConnection hcc;

    private final List<File> outputFiles;

    protected static int DEFAULT_MEM_PAGE_SIZE = 32768;
    protected static int DEFAULT_MEM_NUM_PAGES = 1000;
    protected static double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();

    public AbstractIntegrationTest() {
        outputFiles = new ArrayList<File>();
    }

    @BeforeClass
    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 39001;
        ccConfig.profileDumpPeriod = 10000;
        File outDir = new File("target" + File.separator + "ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(AbstractIntegrationTest.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.ccRoot = ccRoot.getAbsolutePath();
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = "localhost";
        ncConfig1.ccPort = 39001;
        ncConfig1.clusterNetIPAddress = "127.0.0.1";
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.resultIPAddress = "127.0.0.1";
        ncConfig1.nodeId = NC1_ID;
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig();
        ncConfig2.ccHost = "localhost";
        ncConfig2.ccPort = 39001;
        ncConfig2.clusterNetIPAddress = "127.0.0.1";
        ncConfig2.dataIPAddress = "127.0.0.1";
        ncConfig2.resultIPAddress = "127.0.0.1";
        ncConfig2.nodeId = NC2_ID;
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting CC in " + ccRoot.getAbsolutePath());
        }
    }

    @AfterClass
    public static void deinit() throws Exception {
        nc2.stop();
        nc1.stop();
        cc.stop();
    }

    protected JobId executeTest(JobSpecification spec) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(spec.toJSON().toString(2));
        }
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(jobId.toString());
        }
        return jobId;
    }

    protected void runTest(JobSpecification spec) throws Exception {
        JobId jobId = executeTest(spec);
        hcc.waitForCompletion(jobId);
    }


    protected List<String> readResults(JobSpecification spec, JobId jobId, ResultSetId resultSetId) throws Exception {
        int nReaders = 1;

        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor();

        IHyracksDataset hyracksDataset = new HyracksDataset(hcc, spec.getFrameSize(), nReaders);
        IHyracksDatasetReader reader = hyracksDataset.createReader(jobId, resultSetId);

        List<String> resultRecords = new ArrayList<String>();
        ByteBufferInputStream bbis = new ByteBufferInputStream();

        FrameManager resultDisplayFrameMgr = new FrameManager(spec.getFrameSize());
        VSizeFrame frame = new VSizeFrame(resultDisplayFrameMgr);
        int readSize = reader.read(frame);

        while (readSize > 0) {

            try {
                frameTupleAccessor.reset(frame.getBuffer());
                for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                    int start = frameTupleAccessor.getTupleStartOffset(tIndex);
                    int length = frameTupleAccessor.getTupleEndOffset(tIndex) - start;
                    bbis.setByteBuffer(frame.getBuffer(), start);
                    byte[] recordBytes = new byte[length];
                    bbis.read(recordBytes, 0, length);
                    resultRecords.add(new String(recordBytes, 0, length));
                }
            } finally {
                bbis.close();
            }

            readSize = reader.read(frame);
        }
        return resultRecords;
    }

    protected boolean runTestAndCompareResults(JobSpecification spec, String[] expectedFileNames) throws Exception {
        JobId jobId = executeTest(spec);

        List<String> results;
        for (int i = 0; i < expectedFileNames.length; i++) {
            results = readResults(spec, jobId, spec.getResultSetIds().get(i));
            BufferedReader expectedFile = new BufferedReader(new FileReader(expectedFileNames[i]));

            String expectedLine, actualLine;
            int j = 0;
            while ((expectedLine = expectedFile.readLine()) != null) {
                actualLine = results.get(j).trim();
                Assert.assertEquals(expectedLine, actualLine);
                j++;
            }
            Assert.assertEquals(j, results.size());
            expectedFile.close();
        }

        hcc.waitForCompletion(jobId);
        return true;
    }

    protected void runTestAndStoreResult(JobSpecification spec, File file) throws Exception {
        JobId jobId = executeTest(spec);

        BufferedWriter output = new BufferedWriter(new FileWriter(file));
        List<String> results;
        for (int i = 0; i < spec.getResultSetIds().size(); i++) {
            results = readResults(spec, jobId, spec.getResultSetIds().get(i));
            for(String str : results) {
                output.write(str);
            }
        }
        output.close();

        hcc.waitForCompletion(jobId);
    }

    protected File createTempFile() throws IOException {
        File tempFile = File.createTempFile(getClass().getName(), ".tmp", outputFolder.getRoot());
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Output file: " + tempFile.getAbsolutePath());
        }
        outputFiles.add(tempFile);
        return tempFile;
    }
}