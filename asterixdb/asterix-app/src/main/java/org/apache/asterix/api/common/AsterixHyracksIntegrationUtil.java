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
package org.apache.asterix.api.common;

import static org.apache.asterix.api.common.AsterixHyracksIntegrationUtil.LoggerHolder.LOGGER;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.AsterixPropertiesAccessor;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.hyracks.bootstrap.CCApplicationEntryPoint;
import org.apache.asterix.hyracks.bootstrap.NCApplicationEntryPoint;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;

public class AsterixHyracksIntegrationUtil {
    static class LoggerHolder {
        static final Logger LOGGER = Logger.getLogger(AsterixHyracksIntegrationUtil.class.getName());
        private LoggerHolder() {
        }
    }

    private static final String IO_DIR_KEY = "java.io.tmpdir";
    public static final int DEFAULT_HYRACKS_CC_CLIENT_PORT = 1098;
    public static final int DEFAULT_HYRACKS_CC_CLUSTER_PORT = 1099;

    public ClusterControllerService cc;
    public NodeControllerService[] ncs;
    public IHyracksClientConnection hcc;

    private AsterixPropertiesAccessor propertiesAccessor;

    public void init(boolean deleteOldInstanceData) throws Exception {
        ncs = new NodeControllerService[0]; // ensure that ncs is not null
        final CCConfig ccConfig = createCCConfig();
        propertiesAccessor = AsterixPropertiesAccessor.getInstance(ccConfig.getAppConfig());
        if (deleteOldInstanceData) {
            deleteTransactionLogs();
            removeTestStorageFiles();
        }

        cc = new ClusterControllerService(ccConfig);
        cc.start();

        // Starts ncs.
        List<String> nodes = propertiesAccessor.getNodeNames();
        List<NodeControllerService> nodeControllers = new ArrayList<>();
        for (String ncName : nodes) {
            NodeControllerService nodeControllerService =
                    new NodeControllerService(fixupIODevices(createNCConfig(ncName)));
            nodeControllers.add(nodeControllerService);
            Thread ncStartThread = new Thread("IntegrationUtil-" + ncName) {
                @Override
                public void run() {
                    try {
                        nodeControllerService.start();
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, e.getMessage(), e);
                    }
                }
            };
            ncStartThread.start();
            ncStartThread.join();
        }
        hcc = new HyracksConnection(cc.getConfig().clientNetIpAddress, cc.getConfig().clientNetPort);
        ncs = nodeControllers.toArray(new NodeControllerService[nodeControllers.size()]);
    }

    protected CCConfig createCCConfig() throws IOException {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clusterNetIpAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        ccConfig.clientNetIpAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        ccConfig.clientNetPort = DEFAULT_HYRACKS_CC_CLIENT_PORT;
        ccConfig.clusterNetPort = DEFAULT_HYRACKS_CC_CLUSTER_PORT;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.resultTTL = 30000;
        ccConfig.resultSweepThreshold = 1000;
        ccConfig.appCCMainClass = CCApplicationEntryPoint.class.getName();
        return ccConfig;
    }

    protected NCConfig createNCConfig(String ncName) throws AsterixException, IOException {
        NCConfig ncConfig = new NCConfig();
        ncConfig.ccHost = "localhost";
        ncConfig.ccPort = DEFAULT_HYRACKS_CC_CLUSTER_PORT;
        ncConfig.clusterNetIPAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        ncConfig.dataIPAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        ncConfig.resultIPAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        ncConfig.messagingIPAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        ncConfig.nodeId = ncName;
        ncConfig.resultTTL = 30000;
        ncConfig.resultSweepThreshold = 1000;
        ncConfig.appArgs = Collections.singletonList("-virtual-NC");
        ncConfig.appNCMainClass = NCApplicationEntryPoint.class.getName();
        return ncConfig;
    }

    private NCConfig fixupIODevices(NCConfig ncConfig) throws AsterixException {
        String tempPath = System.getProperty(IO_DIR_KEY);
        if (tempPath.endsWith(File.separator)) {
            tempPath = tempPath.substring(0, tempPath.length() - 1);
        }
        LOGGER.info("Using the temp path: " + tempPath);
        // get initial partitions from properties
        String[] nodeStores = propertiesAccessor.getStores().get(ncConfig.nodeId);
        if (nodeStores == null) {
            throw new AsterixException("Couldn't find stores for NC: " + ncConfig.nodeId);
        }
        String tempDirPath = System.getProperty(IO_DIR_KEY);
        if (!tempDirPath.endsWith(File.separator)) {
            tempDirPath += File.separator;
        }
        for (int p = 0; p < nodeStores.length; p++) {
            // create IO devices based on stores
            String iodevicePath = tempDirPath + ncConfig.nodeId + File.separator + nodeStores[p];
            File ioDeviceDir = new File(iodevicePath);
            ioDeviceDir.mkdirs();
            if (p == 0) {
                ncConfig.ioDevices = iodevicePath;
            } else {
                ncConfig.ioDevices += "," + iodevicePath;
            }
        }
        return ncConfig;
    }


    public String[] getNcNames() {
        return propertiesAccessor.getNodeNames().toArray(new String[propertiesAccessor.getNodeNames().size()]);
    }

    public IHyracksClientConnection getHyracksClientConnection() {
        return hcc;
    }

    public void deinit(boolean deleteOldInstanceData) throws Exception {
        //stop NCs
        ArrayList<Thread> stopNCThreads = new ArrayList<>();
        for (NodeControllerService nodeControllerService : ncs) {
            if (nodeControllerService != null) {
                Thread ncStopThread = new Thread() {
                    @Override
                    public void run() {
                        try {
                            nodeControllerService.stop();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
                stopNCThreads.add(ncStopThread);
                ncStopThread.start();
            }
        }

        //make sure all NCs stopped
        for (Thread stopNcTheard : stopNCThreads) {
            stopNcTheard.join();
        }

        if (cc != null) {
            cc.stop();
        }

        if (deleteOldInstanceData) {
            deleteTransactionLogs();
            removeTestStorageFiles();
        }
    }

    public void runJob(JobSpecification spec) throws Exception {
        GlobalConfig.ASTERIX_LOGGER.info(spec.toJSON().toString());
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        GlobalConfig.ASTERIX_LOGGER.info(jobId.toString());
        hcc.waitForCompletion(jobId);
    }

    public void removeTestStorageFiles() {
        File dir = new File(System.getProperty(IO_DIR_KEY));
        for (String ncName : propertiesAccessor.getNodeNames()) {
            File ncDir = new File(dir, ncName);
            FileUtils.deleteQuietly(ncDir);
        }
    }

    private void deleteTransactionLogs() throws IOException {
        for (String ncId : propertiesAccessor.getNodeNames()) {
            File log = new File(propertiesAccessor.getTransactionLogDirs().get(ncId));
            if (log.exists()) {
                FileUtils.deleteDirectory(log);
            }
        }
    }

    /**
     * main method to run a simple 2 node cluster in-process
     * suggested VM arguments: <code>-enableassertions -Xmx2048m -Dfile.encoding=UTF-8</code>
     *
     * @param args
     *            unused
     */
    public static void main(String[] args) throws Exception {
        AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();
        try {
            integrationUtil.run(Boolean.getBoolean("cleanup.start"), Boolean.getBoolean("cleanup.shutdown"));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Unexpected exception", e);
            System.exit(1);
        }
    }

    protected void run(boolean cleanupOnStart, boolean cleanupOnShutdown) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    deinit(cleanupOnShutdown);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Unexpected exception on shutdown", e);
                }
            }
        });
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, "asterix-build-configuration.xml");

        init(cleanupOnStart);
        while (true) {
            Thread.sleep(10000);
        }
    }
}
