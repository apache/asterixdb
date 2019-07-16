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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

import org.apache.asterix.app.external.ExternalUDFLibrarian;
import org.apache.asterix.app.io.PersistedResourceRegistry;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.PropertiesAccessor;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.asterix.hyracks.bootstrap.NCApplication;
import org.apache.asterix.test.dataflow.TestLsmIoOpCallbackFactory;
import org.apache.asterix.test.dataflow.TestPrimaryIndexOperationTrackerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.application.INCApplication;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtreeLocalResource;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;

@SuppressWarnings({ "squid:ClassVariableVisibilityCheck", "squid:S00112" })
public class AsterixHyracksIntegrationUtil {

    public static final int DEFAULT_HYRACKS_CC_CLIENT_PORT = 1098;
    public static final int DEFAULT_HYRACKS_CC_CLUSTER_PORT = 1099;
    public static final String RESOURCES_PATH = joinPath(getProjectPath().toString(), "src", "test", "resources");
    public static final String DEFAULT_CONF_FILE = joinPath(RESOURCES_PATH, "cc.conf");
    private static final String DEFAULT_STORAGE_PATH = joinPath("target", "io", "dir");
    private static String storagePath = DEFAULT_STORAGE_PATH;
    private static final long RESULT_TTL = TimeUnit.MINUTES.toMillis(30);

    static {
        System.setProperty("java.util.logging.manager", org.apache.logging.log4j.jul.LogManager.class.getName());
    }

    public ClusterControllerService cc;
    public NodeControllerService[] ncs = new NodeControllerService[2];
    public IHyracksClientConnection hcc;
    protected boolean gracefulShutdown = true;
    List<Pair<IOption, Object>> opts = new ArrayList<>();
    private ConfigManager configManager;
    private List<String> nodeNames;

    public static void setStoragePath(String path) {
        storagePath = path;
    }

    public static void restoreDefaultStoragePath() {
        storagePath = DEFAULT_STORAGE_PATH;
    }

    /**
     * main method to run a simple 2 node cluster in-process
     * suggested VM arguments: <code>-enableassertions -Xmx2048m -Dfile.encoding=UTF-8</code>
     *
     * @param args
     *            unused
     */
    public static void main(String[] args) throws Exception {
        TestUtils.redirectLoggingToConsole();
        AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();
        try {
            integrationUtil.run(Boolean.getBoolean("cleanup.start"), Boolean.getBoolean("cleanup.shutdown"),
                    System.getProperty("external.lib", ""), System.getProperty("conf.path", DEFAULT_CONF_FILE));
        } catch (Exception e) {
            LOGGER.fatal("Unexpected exception", e);
            System.exit(1);
        }
    }

    public void init(boolean deleteOldInstanceData, String confFile) throws Exception { //NOSONAR
        configureExternalLibDir();

        final ICCApplication ccApplication = createCCApplication();
        if (confFile == null) {
            configManager = new ConfigManager();
        } else {
            configManager = new ConfigManager(new String[] { "-config-file", confFile });
        }
        ccApplication.registerConfig(configManager);
        final CCConfig ccConfig = createCCConfig(configManager);
        configManager.processConfig();
        ccConfig.setKeyStorePath(joinPath(RESOURCES_PATH, ccConfig.getKeyStorePath()));
        ccConfig.setTrustStorePath(joinPath(RESOURCES_PATH, ccConfig.getTrustStorePath()));
        cc = new ClusterControllerService(ccConfig, ccApplication);

        nodeNames = ccConfig.getConfigManager().getNodeNames();
        if (deleteOldInstanceData) {
            deleteTransactionLogs();
            removeTestStorageFiles();
        }
        final List<NodeControllerService> nodeControllers = new ArrayList<>();
        for (String nodeId : nodeNames) {
            // mark this NC as virtual, so that the CC doesn't try to start via NCService...
            configManager.set(nodeId, NCConfig.Option.NCSERVICE_PORT, NCConfig.NCSERVICE_PORT_DISABLED);
            final INCApplication ncApplication = createNCApplication();
            ConfigManager ncConfigManager;
            if (confFile == null) {
                ncConfigManager = new ConfigManager();
            } else {
                ncConfigManager = new ConfigManager(new String[] { "-config-file", confFile });
            }
            ncApplication.registerConfig(ncConfigManager);
            opts.forEach(opt -> ncConfigManager.set(nodeId, opt.getLeft(), opt.getRight()));
            nodeControllers
                    .add(new NodeControllerService(fixupPaths(createNCConfig(nodeId, ncConfigManager)), ncApplication));
        }

        opts.forEach(opt -> configManager.set(opt.getLeft(), opt.getRight()));
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
                        LOGGER.log(Level.ERROR, e.getMessage(), e);
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
        ((ICcApplicationContext) cc.getApplicationContext()).getClusterStateManager().waitForState(ClusterState.ACTIVE);
        hcc = new HyracksConnection(cc.getConfig().getClientListenAddress(), cc.getConfig().getClientListenPort(),
                cc.getNetworkSecurityManager().getSocketChannelFactory());
        this.ncs = nodeControllers.toArray(new NodeControllerService[nodeControllers.size()]);
    }

    private void configureExternalLibDir() {
        // hack to ensure we have a unique location for external libraries in our tests (asterix cluster has a shared
        // home directory)-- TODO: rework this once the external lib dir can be configured explicitly
        String appHome = joinPath(System.getProperty("app.home", System.getProperty("user.home")),
                "appHome" + (int) (Math.random() * Integer.MAX_VALUE));
        LOGGER.info("setting app.home to {}", appHome);
        System.setProperty("app.home", appHome);
        new File(appHome).deleteOnExit();
    }

    public void init(boolean deleteOldInstanceData, String externalLibPath, String confDir) throws Exception {
        List<ILibraryManager> libraryManagers = new ArrayList<>();
        ExternalUDFLibrarian librarian = new ExternalUDFLibrarian();
        init(deleteOldInstanceData, confDir);
        if (externalLibPath != null && externalLibPath.length() != 0) {
            libraryManagers.add(((ICcApplicationContext) cc.getApplicationContext()).getLibraryManager());
            for (NodeControllerService nc : ncs) {
                INcApplicationContext runtimeCtx = (INcApplicationContext) nc.getApplicationContext();
                libraryManagers.add(runtimeCtx.getLibraryManager());
            }
            librarian.install(System.getProperty("external.lib.dataverse", "test"),
                    System.getProperty("external.lib.libname", "testlib"), externalLibPath);
        }
    }

    public ClusterControllerService getClusterControllerService() {
        return cc;
    }

    protected CCConfig createCCConfig(ConfigManager configManager) throws IOException {
        CCConfig ccConfig = new CCConfig(configManager);
        ccConfig.setClusterListenAddress(Inet4Address.getLoopbackAddress().getHostAddress());
        ccConfig.setClientListenAddress(Inet4Address.getLoopbackAddress().getHostAddress());
        ccConfig.setClientListenPort(DEFAULT_HYRACKS_CC_CLIENT_PORT);
        ccConfig.setClusterListenPort(DEFAULT_HYRACKS_CC_CLUSTER_PORT);
        ccConfig.setResultTTL(RESULT_TTL);
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
        ncConfig.setResultTTL(RESULT_TTL);
        ncConfig.setResultSweepThreshold(1000L);
        ncConfig.setVirtualNC();
        configManager.set(ControllerConfig.Option.DEFAULT_DIR, joinPath(getDefaultStoragePath(), "asterixdb", ncName));
        return ncConfig;
    }

    protected INCApplication createNCApplication() {
        // Instead of using this flag, RecoveryManagerTest should set the desired class in its config file
        if (!gracefulShutdown) {
            return new UngracefulShutdownNCApplication();
        }
        return new TestNCApplication();
    }

    private NCConfig fixupPaths(NCConfig ncConfig) throws IOException, AsterixException, CmdLineException {
        // we have to first process the config
        ncConfig.getConfigManager().processConfig();

        // get initial partitions from config
        String[] nodeStores = ncConfig.getNodeScopedAppConfig().getStringArray(NCConfig.Option.IODEVICES);
        if (nodeStores == null) {
            throw new IllegalStateException("Couldn't find stores for NC: " + ncConfig.getNodeId());
        }
        LOGGER.info("Using the path: " + getDefaultStoragePath());
        for (int i = 0; i < nodeStores.length; i++) {
            // create IO devices based on stores
            nodeStores[i] = joinPath(getDefaultStoragePath(), ncConfig.getNodeId(), nodeStores[i]);
        }
        ncConfig.getConfigManager().set(ncConfig.getNodeId(), NCConfig.Option.IODEVICES, nodeStores);
        final String keyStorePath = joinPath(RESOURCES_PATH, ncConfig.getKeyStorePath());
        final String trustStorePath = joinPath(RESOURCES_PATH, ncConfig.getTrustStorePath());
        ncConfig.getConfigManager().set(ncConfig.getNodeId(), NCConfig.Option.KEY_STORE_PATH, keyStorePath);
        ncConfig.getConfigManager().set(ncConfig.getNodeId(), NCConfig.Option.TRUST_STORE_PATH, trustStorePath);
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

    public void setGracefulShutdown(boolean gracefulShutdown) {
        this.gracefulShutdown = gracefulShutdown;
    }

    protected String getDefaultStoragePath() {
        return storagePath;
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

    protected void run(boolean cleanupOnStart, boolean cleanupOnShutdown, String loadExternalLibs, String confFile)
            throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    deinit(cleanupOnShutdown);
                } catch (Exception e) {
                    LOGGER.log(Level.WARN, "Unexpected exception on shutdown", e);
                }
            }
        });

        init(cleanupOnStart, loadExternalLibs, confFile);
        while (true) {
            Thread.sleep(10000);
        }
    }

    protected void run(boolean cleanupOnStart, boolean cleanupOnShutdown, String loadExternalLibs) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    deinit(cleanupOnShutdown);
                } catch (Exception e) {
                    LOGGER.log(Level.WARN, "Unexpected exception on shutdown", e);
                }
            }
        });

        init(cleanupOnStart, loadExternalLibs);
        while (true) {
            Thread.sleep(10000);
        }
    }

    public void addOption(IOption name, Object value) {
        opts.add(Pair.of(name, value));
    }

    public void clearOptions() {
        opts.clear();
    }

    /**
     * @return the asterix-app absolute path if found, otherwise the default user path.
     */
    private static Path getProjectPath() {
        final String targetDir = "asterix-app";
        final BiPredicate<Path, BasicFileAttributes> matcher =
                (path, attributes) -> path.getFileName().toString().equals(targetDir) && path.toFile().isDirectory()
                        && path.toFile().list((d, n) -> n.equals("pom.xml")).length == 1;
        final Path currentPath = Paths.get(System.getProperty("user.dir"));
        try (Stream<Path> pathStream = Files.find(currentPath, 10, matcher)) {
            return pathStream.findFirst().orElse(currentPath);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static class LoggerHolder {
        static final Logger LOGGER = LogManager.getLogger();

        private LoggerHolder() {
        }
    }

    private class TestNCApplication extends NCApplication {
        @Override
        protected void configurePersistedResourceRegistry() {
            ncServiceCtx.setPersistedResourceRegistry(new TestPersistedResourceRegistry());
        }
    }

    private class UngracefulShutdownNCApplication extends TestNCApplication {
        @Override
        public void stop() throws Exception {
            // ungraceful shutdown
            webManager.stop();
        }
    }

    private static class TestPersistedResourceRegistry extends PersistedResourceRegistry {
        @Override
        protected void registerClasses() {
            super.registerClasses();
            registeredClasses.put("TestLsmBtreeLocalResource", TestLsmBtreeLocalResource.class);
            registeredClasses.put("TestLsmIoOpCallbackFactory", TestLsmIoOpCallbackFactory.class);
            registeredClasses.put("TestPrimaryIndexOperationTrackerFactory",
                    TestPrimaryIndexOperationTrackerFactory.class);
        }
    }
}
