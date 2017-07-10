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
import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.config.PropertiesAccessor;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.asterix.hyracks.bootstrap.NCApplication;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.application.INCApplication;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.kohsuke.args4j.CmdLineException;

public class AsterixHyracksIntegrationUtil {
    static class LoggerHolder {
        static final Logger LOGGER = Logger.getLogger(AsterixHyracksIntegrationUtil.class.getName());

        private LoggerHolder() {
        }
    }

    public static final int DEFAULT_HYRACKS_CC_CLIENT_PORT = 1098;
    public static final int DEFAULT_HYRACKS_CC_CLUSTER_PORT = 1099;

    public ClusterControllerService cc;
    public NodeControllerService[] ncs = new NodeControllerService[0];
    public IHyracksClientConnection hcc;

    private ConfigManager configManager;
    private List<String> nodeNames;

    public void init(boolean deleteOldInstanceData) throws Exception {
        final ICCApplication ccApplication = createCCApplication();
        configManager = new ConfigManager();
        ccApplication.registerConfig(configManager);
        final CCConfig ccConfig = createCCConfig(configManager);
        cc = new ClusterControllerService(ccConfig, ccApplication);

        nodeNames = ccConfig.getConfigManager().getNodeNames();
        if (deleteOldInstanceData) {
            deleteTransactionLogs();
            removeTestStorageFiles();
        }
        final List<NodeControllerService> nodeControllers = new ArrayList<>();
        for (String nodeId : nodeNames) {
            // mark this NC as virtual in the CC's config manager, so he doesn't try to contact NCService...
            configManager.set(nodeId, NCConfig.Option.NCSERVICE_PORT, NCConfig.NCSERVICE_PORT_DISABLED);
            final INCApplication ncApplication = createNCApplication();
            ConfigManager ncConfigManager = new ConfigManager();
            ncApplication.registerConfig(ncConfigManager);
            nodeControllers.add(
                    new NodeControllerService(fixupIODevices(createNCConfig(nodeId, ncConfigManager)), ncApplication));
        } ;

        cc.start();

        // Starts ncs.
        nodeNames = ccConfig.getConfigManager().getNodeNames();
        List<Thread> startupThreads = new ArrayList<>();
        for (NodeControllerService nc : nodeControllers) {
            Thread ncStartThread = new Thread("IntegrationUtil-" + nc.getId()) {
                @Override
                public void run() {
                    try {
                        nc.start();
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
        // Wait until cluster becomes active
        ClusterStateManager.INSTANCE.waitForState(ClusterState.ACTIVE);
        hcc = new HyracksConnection(cc.getConfig().getClientListenAddress(), cc.getConfig().getClientListenPort());
        this.ncs = nodeControllers.toArray(new NodeControllerService[nodeControllers.size()]);
    }

    protected CCConfig createCCConfig(ConfigManager configManager) throws IOException {
        CCConfig ccConfig = new CCConfig(configManager);
        ccConfig.setClusterListenAddress(Inet4Address.getLoopbackAddress().getHostAddress());
        ccConfig.setClientListenAddress(Inet4Address.getLoopbackAddress().getHostAddress());
        ccConfig.setClientListenPort(DEFAULT_HYRACKS_CC_CLIENT_PORT);
        ccConfig.setClusterListenPort(DEFAULT_HYRACKS_CC_CLUSTER_PORT);
        ccConfig.setResultTTL(120000L);
        ccConfig.setResultSweepThreshold(1000L);
        ccConfig.setEnforceFrameWriterProtocol(true);
        configManager.set(ControllerConfig.Option.DEFAULT_DIR, joinPath(getDefaultStoragePath(), "asterixdb"));
        return ccConfig;
    }

    protected ICCApplication createCCApplication() {
        return new CCApplication();
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
        ncConfig.setVirtualNC();
        configManager.set(ControllerConfig.Option.DEFAULT_DIR, joinPath(getDefaultStoragePath(), "asterixdb", ncName));
        return ncConfig;
    }

    protected INCApplication createNCApplication() {
        return new NCApplication();
    }

    private NCConfig fixupIODevices(NCConfig ncConfig) throws IOException, AsterixException, CmdLineException {
        // we have to first process the config
        ncConfig.getConfigManager().processConfig();

        // get initial partitions from config
        String[] nodeStores = ncConfig.getAppConfig().getStringArray(NCConfig.Option.IODEVICES);
        if (nodeStores == null) {
            throw new IllegalStateException("Couldn't find stores for NC: " + ncConfig.getNodeId());
        }
        LOGGER.info("Using the path: " + getDefaultStoragePath());
        for (int i = 0; i < nodeStores.length; i++) {
            // create IO devices based on stores
            nodeStores[i] = joinPath(getDefaultStoragePath(), ncConfig.getNodeId(), nodeStores[i]);
        }
        ncConfig.getConfigManager().set(ncConfig.getNodeId(), NCConfig.Option.IODEVICES, nodeStores);
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

        stopCC(false);

        if (deleteOldInstanceData) {
            deleteTransactionLogs();
            removeTestStorageFiles();
        }
    }

    public void stopCC(boolean terminateNCService) throws Exception {
        if (cc != null) {
            cc.stop(terminateNCService);
            cc = null;
        }
    }

    protected String getDefaultStoragePath() {
        return joinPath("target", "io", "dir");
    }

    public void removeTestStorageFiles() {
        File dir = new File(getDefaultStoragePath());
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
