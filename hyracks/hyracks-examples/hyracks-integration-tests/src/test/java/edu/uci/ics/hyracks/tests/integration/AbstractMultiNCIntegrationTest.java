/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.tests.integration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetReader;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public abstract class AbstractMultiNCIntegrationTest {

    private static final Logger LOGGER = Logger.getLogger(AbstractMultiNCIntegrationTest.class.getName());

    public static final String[] ASTERIX_IDS = { "asterix-001", "asterix-002", "asterix-003", "asterix-004",
            "asterix-005", "asterix-006", "asterix-007" };

    private static ClusterControllerService cc;

    private static NodeControllerService[] asterixNCs;

    private static IHyracksClientConnection hcc;

    private final List<File> outputFiles;

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();

    public AbstractMultiNCIntegrationTest() {
        outputFiles = new ArrayList<File>();;
    }

    @BeforeClass
    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 39001;
        ccConfig.profileDumpPeriod = 10000;
        File outDir = new File("target/ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(AbstractMultiNCIntegrationTest.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.ccRoot = ccRoot.getAbsolutePath();
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        asterixNCs = new NodeControllerService[ASTERIX_IDS.length];
        for (int i = 0; i < ASTERIX_IDS.length; i++) {
            NCConfig ncConfig = new NCConfig();
            ncConfig.ccHost = "localhost";
            ncConfig.ccPort = 39001;
            ncConfig.clusterNetIPAddress = "127.0.0.1";
            ncConfig.dataIPAddress = "127.0.0.1";
            ncConfig.datasetIPAddress = "127.0.0.1";
            ncConfig.nodeId = ASTERIX_IDS[i];
            asterixNCs[i] = new NodeControllerService(ncConfig);
            asterixNCs[i].start();
        }

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting CC in " + ccRoot.getAbsolutePath());
        }
    }

    @AfterClass
    public static void deinit() throws Exception {
        for (NodeControllerService nc : asterixNCs) {
            nc.stop();
        }
        cc.stop();
    }

    protected void runTest(JobSpecification spec) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(spec.toJSON().toString(2));
        }
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(jobId.toString());
        }

        int nReaders = 1;

        ByteBuffer resultBuffer = ByteBuffer.allocate(spec.getFrameSize());
        resultBuffer.clear();

        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor(spec.getFrameSize());

        IHyracksDataset hyracksDataset = new HyracksDataset(hcc, spec.getFrameSize(), nReaders);
        IHyracksDatasetReader reader = hyracksDataset.createReader(jobId, spec.getResultSetIds().get(0));

        JSONArray resultRecords = new JSONArray();
        ByteBufferInputStream bbis = new ByteBufferInputStream();

        int readSize = reader.read(resultBuffer);

        while (readSize > 0) {

            try {
                frameTupleAccessor.reset(resultBuffer);
                for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                    int start = frameTupleAccessor.getTupleStartOffset(tIndex);
                    int length = frameTupleAccessor.getTupleEndOffset(tIndex) - start;
                    bbis.setByteBuffer(resultBuffer, start);
                    byte[] recordBytes = new byte[length];
                    bbis.read(recordBytes, 0, length);
                    resultRecords.put(new String(recordBytes, 0, length));
                }
            } finally {
                try {
                    bbis.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            resultBuffer.clear();
            readSize = reader.read(resultBuffer);
        }

        hcc.waitForCompletion(jobId);
        dumpOutputFiles();
    }

    private void dumpOutputFiles() {
        if (LOGGER.isLoggable(Level.INFO)) {
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

    protected File createTempFile() throws IOException {
        File tempFile = File.createTempFile(getClass().getName(), ".tmp", outputFolder.getRoot());
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Output file: " + tempFile.getAbsolutePath());
        }
        outputFiles.add(tempFile);
        return tempFile;
    }

}
