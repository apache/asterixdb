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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.IResultSetReader;
import org.apache.hyracks.client.result.ResultSet;
import org.apache.hyracks.control.cc.BaseCCApplication;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.application.CCServiceContext;
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
import org.junit.BeforeClass;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public abstract class AbstractMultiNCIntegrationTest {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final TestJobLifecycleListener jobLifecycleListener = new TestJobLifecycleListener();

    public static final String[] ASTERIX_IDS =
            { "asterix-001", "asterix-002", "asterix-003", "asterix-004", "asterix-005", "asterix-006", "asterix-007" };

    protected static ClusterControllerService cc;

    protected static NodeControllerService[] asterixNCs;

    protected static IHyracksClientConnection hcc;

    protected final List<File> outputFiles;

    public AbstractMultiNCIntegrationTest() {
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
        ccConfig.setJobHistorySize(2);
        File outDir = new File("target" + File.separator + "ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(AbstractMultiNCIntegrationTest.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.setRootDir(ccRoot.getAbsolutePath());
        ccConfig.setAppClass(DummyApplication.class.getName());
        cc = new ClusterControllerService(ccConfig);
        cc.start();
        CCServiceContext serviceCtx = cc.getContext();
        serviceCtx.addJobLifecycleListener(jobLifecycleListener);
        asterixNCs = new NodeControllerService[ASTERIX_IDS.length];
        for (int i = 0; i < ASTERIX_IDS.length; i++) {
            File ioDev = new File("target" + File.separator + ASTERIX_IDS[i] + File.separator + "ioDevice");
            FileUtils.forceMkdir(ioDev);
            FileUtils.copyDirectory(new File("data" + File.separator + "device0"), ioDev);
            NCConfig ncConfig = new NCConfig(ASTERIX_IDS[i]);
            ncConfig.setClusterAddress("localhost");
            ncConfig.setClusterPort(39001);
            ncConfig.setClusterListenAddress("127.0.0.1");
            ncConfig.setDataListenAddress("127.0.0.1");
            ncConfig.setResultListenAddress("127.0.0.1");
            ncConfig.setIODevices(new String[] { ioDev.getAbsolutePath() });
            asterixNCs[i] = new NodeControllerService(ncConfig);
            asterixNCs[i].start();
        }

        hcc = new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting CC in " + ccRoot.getAbsolutePath());
        }
    }

    @AfterClass
    public static void deinit() throws Exception {
        for (NodeControllerService nc : asterixNCs) {
            nc.stop();
        }
        cc.stop();
        jobLifecycleListener.check();
    }

    protected JobId startJob(JobSpecification spec) throws Exception {
        return hcc.startJob(spec);
    }

    protected void waitForCompletion(JobId jobId) throws Exception {
        hcc.waitForCompletion(jobId);
    }

    protected JobStatus getJobStatus(JobId jobId) throws Exception {
        return hcc.getJobStatus(jobId);
    }

    protected void cancelJob(JobId jobId) throws Exception {
        hcc.cancelJob(jobId);
    }

    protected JobId runTest(JobSpecification spec, String expectedErrorMessage) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(spec.toJSON().asText());
        }
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(jobId.toString());
        }

        int nReaders = 1;

        FrameManager resultDisplayFrameMgr = new FrameManager(spec.getFrameSize());
        VSizeFrame resultFrame = new VSizeFrame(resultDisplayFrameMgr);

        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor();

        if (!spec.getResultSetIds().isEmpty()) {
            IResultSet resultSet =
                    new ResultSet(hcc, PlainSocketChannelFactory.INSTANCE, spec.getFrameSize(), nReaders);
            IResultSetReader reader = resultSet.createReader(jobId, spec.getResultSetIds().get(0));

            ObjectMapper om = new ObjectMapper();
            ArrayNode resultRecords = om.createArrayNode();
            ByteBufferInputStream bbis = new ByteBufferInputStream();

            int readSize = reader.read(resultFrame);

            while (readSize > 0) {

                try {
                    frameTupleAccessor.reset(resultFrame.getBuffer());
                    for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                        int start = frameTupleAccessor.getTupleStartOffset(tIndex);
                        int length = frameTupleAccessor.getTupleEndOffset(tIndex) - start;
                        bbis.setByteBuffer(resultFrame.getBuffer(), start);
                        byte[] recordBytes = new byte[length];
                        bbis.read(recordBytes, 0, length);
                        resultRecords.add(new String(recordBytes, 0, length));
                    }
                } finally {
                    try {
                        bbis.close();
                    } catch (IOException e) {
                        throw HyracksDataException.create(e);
                    }
                }
                readSize = reader.read(resultFrame);
            }
        }
        waitForCompletion(jobId, expectedErrorMessage);
        // Waiting a second time should lead to the same behavior
        waitForCompletion(jobId, expectedErrorMessage);
        dumpOutputFiles();
        return jobId;
    }

    protected void waitForCompletion(JobId jobId, String expectedErrorMessage) throws Exception {
        boolean expectedExceptionThrown = false;
        try {
            hcc.waitForCompletion(jobId);
        } catch (Exception e) {
            if (expectedErrorMessage != null) {
                if (e.toString().contains(expectedErrorMessage)) {
                    expectedExceptionThrown = true;
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }
        if (expectedErrorMessage != null && !expectedExceptionThrown) {
            throw new Exception("Expected error (" + expectedErrorMessage + ") was not thrown");
        }
    }

    private void dumpOutputFiles() {
        if (LOGGER.isInfoEnabled()) {
            for (File f : outputFiles) {
                if (f.exists() && f.isFile()) {
                    try {
                        LOGGER.info("Reading file: " + f.getAbsolutePath() + " in test: " + getClass().getName());
                        String data = FileUtils.readFileToString(f);
                        LOGGER.info(data);
                    } catch (IOException e) {
                        LOGGER.info("Error reading file: " + f.getAbsolutePath());
                        LOGGER.info(e.getMessage());
                    }
                }
            }
        }
    }

    public static class DummyApplication extends BaseCCApplication {

        @Override
        public IJobCapacityController getJobCapacityController() {
            return new IJobCapacityController() {
                private long maxRAM = Runtime.getRuntime().maxMemory();

                @Override
                public JobSubmissionStatus allocate(JobSpecification job) throws HyracksException {
                    return maxRAM > job.getRequiredClusterCapacity().getAggregatedMemoryByteSize()
                            ? JobSubmissionStatus.EXECUTE : JobSubmissionStatus.QUEUE;
                }

                @Override
                public void release(JobSpecification job) {

                }
            };
        }
    }

}
