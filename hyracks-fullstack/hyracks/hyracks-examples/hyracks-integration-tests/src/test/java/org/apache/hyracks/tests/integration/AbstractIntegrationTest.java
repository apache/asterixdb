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

import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.IResultSetReader;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.client.result.ResultSet;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.ipc.sockets.PlainSocketChannelFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

public abstract class AbstractIntegrationTest {
    private static final Logger LOGGER = LogManager.getLogger();

    public static final String NC1_ID = "nc1";
    public static final String NC2_ID = "nc2";

    protected static ClusterControllerService cc;
    protected static NodeControllerService nc1;
    protected static NodeControllerService nc2;
    protected static IHyracksClientConnection hcc;

    private final List<File> outputFiles;
    private static AtomicInteger aInteger = new AtomicInteger(0);

    protected static int DEFAULT_MEM_PAGE_SIZE = 32768;
    protected static int DEFAULT_MEM_NUM_PAGES = 1000;
    protected static double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;

    public AbstractIntegrationTest() {
        outputFiles = new ArrayList<>();
    }

    @BeforeClass
    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.setClientListenAddress("127.0.0.1");
        ccConfig.setClientListenPort(39000);
        ccConfig.setClusterListenAddress("127.0.0.1");
        ccConfig.setClusterListenPort(39001);
        ccConfig.setProfileDumpPeriod(10000);
        FileUtils.deleteQuietly(new File("target" + File.separator + "data"));
        FileUtils.copyDirectory(new File("data"), new File(joinPath("target", "data")));
        File outDir = new File("target" + File.separator + "ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(AbstractIntegrationTest.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.setRootDir(ccRoot.getAbsolutePath());
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        NCConfig ncConfig1 = new NCConfig(NC1_ID);
        ncConfig1.setClusterAddress("localhost");
        ncConfig1.setClusterPort(39001);
        ncConfig1.setClusterListenAddress("127.0.0.1");
        ncConfig1.setDataListenAddress("127.0.0.1");
        ncConfig1.setResultListenAddress("127.0.0.1");
        ncConfig1.setIODevices(new String[] { joinPath(System.getProperty("user.dir"), "target", "data", "device0") });
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig(NC2_ID);
        ncConfig2.setClusterAddress("localhost");
        ncConfig2.setClusterPort(39001);
        ncConfig2.setClusterListenAddress("127.0.0.1");
        ncConfig2.setDataListenAddress("127.0.0.1");
        ncConfig2.setResultListenAddress("127.0.0.1");
        ncConfig2.setIODevices(new String[] { joinPath(System.getProperty("user.dir"), "target", "data", "device1") });
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();

        hcc = new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());
        if (LOGGER.isInfoEnabled()) {
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
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(spec.toJSON().asText());
        }
        JobId jobId = hcc.startJob(spec);
        if (LOGGER.isInfoEnabled()) {
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

        IResultSet resultSet = new ResultSet(hcc, PlainSocketChannelFactory.INSTANCE, spec.getFrameSize(), nReaders);
        IResultSetReader reader = resultSet.createReader(jobId, resultSetId);

        List<String> resultRecords = new ArrayList<>();
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

            //We're expecting some sort of result.
            Assert.assertTrue(results != null);
            Assert.assertTrue(results.size() > 0);

            String expectedLine, actualLine;
            int j = 0;
            while ((expectedLine = expectedFile.readLine()) != null) {
                actualLine = results.get(j).trim();
                Assert.assertEquals(expectedLine, actualLine);
                j++;
            }
            //We also expect the same amount of results.
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
            for (String str : results) {
                output.write(str);
            }
        }
        output.close();

        hcc.waitForCompletion(jobId);
    }

    protected FileSplit createFile(NodeControllerService ncs) throws IOException {
        String fileName = "f" + aInteger.getAndIncrement() + ".tmp";
        FileReference fileRef = ncs.getIoManager().getFileReference(0, fileName);
        FileUtils.deleteQuietly(fileRef.getFile());
        fileRef.getFile().createNewFile();
        outputFiles.add(fileRef.getFile());
        return new ManagedFileSplit(ncs.getId(), fileName);
    }
}
