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

import java.io.File;
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
    private static final Logger LOGGER = Logger.getLogger(AsterixHyracksIntegrationUtil.class.getName());
    private static final String IO_DIR_KEY = "java.io.tmpdir";
    public static final int DEFAULT_HYRACKS_CC_CLIENT_PORT = 1098;
    public static final int DEFAULT_HYRACKS_CC_CLUSTER_PORT = 1099;

    public ClusterControllerService cc;
    public NodeControllerService[] ncs;
    public IHyracksClientConnection hcc;

    private AsterixPropertiesAccessor propertiesAccessor;

    public void init(boolean deleteOldInstanceData) throws Exception {
        ncs = new NodeControllerService[0]; // ensure that ncs is not null
        propertiesAccessor = new AsterixPropertiesAccessor();
        if (deleteOldInstanceData) {
            deleteTransactionLogs();
            removeTestStorageFiles();
        }

        cc = new ClusterControllerService(createCCConfig());
        cc.start();

        // Starts ncs.
        List<String> nodes = propertiesAccessor.getNodeNames();
        List<NodeControllerService> nodeControllers = new ArrayList<>();
        for (String ncName : nodes) {
            NodeControllerService nodeControllerService = new NodeControllerService(createNCConfig(ncName));
            nodeControllers.add(nodeControllerService);
            Thread ncStartThread = new Thread() {
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

    protected CCConfig createCCConfig() {
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

    protected NCConfig createNCConfig(String ncName) throws AsterixException {
        NCConfig ncConfig = new NCConfig();
        ncConfig.ccHost = "localhost";
        ncConfig.ccPort = DEFAULT_HYRACKS_CC_CLUSTER_PORT;
        ncConfig.clusterNetIPAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        ncConfig.dataIPAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        ncConfig.resultIPAddress = Inet4Address.getLoopbackAddress().getHostAddress();
        ncConfig.nodeId = ncName;
        ncConfig.resultTTL = 30000;
        ncConfig.resultSweepThreshold = 1000;
        ncConfig.appArgs = Collections.singletonList("-virtual-NC");
        String tempPath = System.getProperty(IO_DIR_KEY);
        if (tempPath.endsWith(File.separator)) {
            tempPath = tempPath.substring(0, tempPath.length() - 1);
        }
        System.err.println("Using the path: " + tempPath);
        // get initial partitions from properties
        String[] nodeStores = propertiesAccessor.getStores().get(ncName);
        if (nodeStores == null) {
            throw new AsterixException("Coudn't find stores for NC: " + ncName);
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
        ncConfig.appNCMainClass = NCApplicationEntryPoint.class.getName();
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

    private void deleteTransactionLogs() throws Exception {
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
    public static void main(String[] args) {
        AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();
        run(integrationUtil);
    }

    protected static void run(final AsterixHyracksIntegrationUtil integrationUtil) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    integrationUtil.deinit(false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        try {
            System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, "asterix-build-configuration.xml");

            integrationUtil.init(false);
            while (true) {
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
