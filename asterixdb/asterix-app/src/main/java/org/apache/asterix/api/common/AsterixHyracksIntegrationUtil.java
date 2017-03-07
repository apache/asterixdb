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
import java.util.EnumSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.config.PropertiesAccessor;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.hyracks.bootstrap.CCApplicationEntryPoint;
import org.apache.asterix.hyracks.bootstrap.NCApplicationEntryPoint;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.application.ICCApplicationEntryPoint;
import org.apache.hyracks.api.application.INCApplicationEntryPoint;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.config.ConfigManager;
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

    private ConfigManager configManager;
    private List<String> nodeNames;

    public void init(boolean deleteOldInstanceData) throws Exception {
        ncs = new NodeControllerService[0]; // ensure that ncs is not null
        final ICCApplicationEntryPoint ccAppEntryPoint = createCCAppEntryPoint();
        configManager = new ConfigManager();
        ccAppEntryPoint.registerConfig(configManager);
        final CCConfig ccConfig = createCCConfig(configManager);
        cc = new ClusterControllerService(ccConfig, ccAppEntryPoint);

        nodeNames = ccConfig.getConfigManager().getNodeNames();
        if (deleteOldInstanceData) {
            deleteTransactionLogs();
            removeTestStorageFiles();
        }
        final List<NCConfig> ncConfigs = new ArrayList<>();
        nodeNames.forEach(nodeId -> ncConfigs.add(createNCConfig(nodeId, configManager)));
        final PropertiesAccessor accessor = PropertiesAccessor.getInstance(configManager.getAppConfig());
        ncConfigs.forEach((ncConfig1) -> fixupIODevices(ncConfig1, accessor));

        cc.start();

        // Starts ncs.
        nodeNames = ccConfig.getConfigManager().getNodeNames();
        List<NodeControllerService> nodeControllers = new ArrayList<>();
        List<Thread> startupThreads = new ArrayList<>();
        for (NCConfig ncConfig : ncConfigs) {
            final INCApplicationEntryPoint ncAppEntryPoint = createNCAppEntryPoint();
            NodeControllerService nodeControllerService = new NodeControllerService(ncConfig, ncAppEntryPoint);
            nodeControllers.add(nodeControllerService);
            Thread ncStartThread = new Thread("IntegrationUtil-" + ncConfig.getNodeId()) {
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
            startupThreads.add(ncStartThread);
        }
        //wait until all NCs complete their startup
        for (Thread thread : startupThreads) {
            thread.join();
        }
        for (NCConfig ncConfig : ncConfigs) {
            for (String ioDevice : ncConfig.getIODevices()) {
                if (!new File(ioDevice).isAbsolute()) {
                    throw new IllegalStateException("iodevice not absolute: " + ioDevice);
                }
            }
        }
        // Wait until cluster becomes active
        synchronized (ClusterStateManager.INSTANCE) {
            while (ClusterStateManager.INSTANCE.getState() != ClusterState.ACTIVE) {
                ClusterStateManager.INSTANCE.wait();
            }
        }
        hcc = new HyracksConnection(cc.getConfig().getClientListenAddress(), cc.getConfig().getClientListenPort());
        ncs = nodeControllers.toArray(new NodeControllerService[nodeControllers.size()]);
    }

    protected CCConfig createCCConfig(ConfigManager configManager) throws IOException {
        CCConfig ccConfig = new CCConfig(configManager);
        ccConfig.setClusterListenAddress(Inet4Address.getLoopbackAddress().getHostAddress());
        ccConfig.setClientListenAddress(Inet4Address.getLoopbackAddress().getHostAddress());
        ccConfig.setClientListenPort(DEFAULT_HYRACKS_CC_CLIENT_PORT);
        ccConfig.setClusterListenPort(DEFAULT_HYRACKS_CC_CLUSTER_PORT);
        ccConfig.setResultTTL(120000L);
        ccConfig.setResultSweepThreshold(1000L);
        return ccConfig;
    }

    protected ICCApplicationEntryPoint createCCAppEntryPoint() {
        return new CCApplicationEntryPoint();
    }

    protected NCConfig createNCConfig(String ncName, ConfigManager configManager) {
        NCConfig ncConfig = new NCConfig(ncName, configManager);
        ncConfig.setClusterAddress("localhost");
        ncConfig.setClusterPort(DEFAULT_HYRACKS_CC_CLUSTER_PORT);
        ncConfig.setClusterListenAddress(Inet4Address.getLoopbackAddress().getHostAddress());
        ncConfig.setDataListenAddress(Inet4Address.getLoopbackAddress().getHostAddress());
        ncConfig.setResultListenAddress(Inet4Address.getLoopbackAddress().getHostAddress());
        ncConfig.setMessagingListenAddress(Inet4Address.getLoopbackAddress().getHostAddress());
        ncConfig.setResultTTL(120000L);
        ncConfig.setResultSweepThreshold(1000L);
        ncConfig.setVirtualNC(true);
        return ncConfig;
    }

    protected INCApplicationEntryPoint createNCAppEntryPoint() {
        return new NCApplicationEntryPoint();
    }

    private NCConfig fixupIODevices(NCConfig ncConfig, PropertiesAccessor accessor) {
        String tempPath = System.getProperty(IO_DIR_KEY);
        if (tempPath.endsWith(File.separator)) {
            tempPath = tempPath.substring(0, tempPath.length() - 1);
        }
        LOGGER.info("Using the temp path: " + tempPath);
        // get initial partitions from properties
        String[] nodeStores = accessor.getStores().get(ncConfig.getNodeId());
        if (nodeStores == null) {
            throw new IllegalStateException("Couldn't find stores for NC: " + ncConfig.getNodeId());
        }
        String tempDirPath = System.getProperty(IO_DIR_KEY);
        if (!tempDirPath.endsWith(File.separator)) {
            tempDirPath += File.separator;
        }
        List<String> ioDevices = new ArrayList<>();
        for (String nodeStore : nodeStores) {
            // create IO devices based on stores
            String iodevicePath = tempDirPath + ncConfig.getNodeId() + File.separator + nodeStore;
            File ioDeviceDir = new File(iodevicePath);
            ioDeviceDir.mkdirs();
            ioDevices.add(iodevicePath);
        }
        configManager.set(ncConfig.getNodeId(), NCConfig.Option.IODEVICES, ioDevices.toArray(new String[0]));
        return ncConfig;
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
        for (String ncName : nodeNames) {
            File ncDir = new File(dir, ncName);
            FileUtils.deleteQuietly(ncDir);
        }
    }

    private void deleteTransactionLogs() throws IOException, AsterixException {
        for (String ncId : nodeNames) {
            File log = new File(
                    PropertiesAccessor.getInstance(configManager.getAppConfig()).getTransactionLogDirs().get(ncId));
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
