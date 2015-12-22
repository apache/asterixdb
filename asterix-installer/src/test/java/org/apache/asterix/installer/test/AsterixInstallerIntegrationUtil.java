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
package org.apache.asterix.installer.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.event.error.VerificationUtil;
import org.apache.asterix.event.model.AsterixInstance;
import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.event.model.AsterixRuntimeState;
import org.apache.asterix.event.service.ServiceProvider;
import org.apache.asterix.installer.command.CommandHandler;
import org.apache.asterix.installer.driver.InstallerDriver;
import org.apache.asterix.installer.schema.conf.Configuration;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;

public class AsterixInstallerIntegrationUtil {

    private static String managixHome;
    private static String clusterConfigurationPath;
    private static final CommandHandler cmdHandler = new CommandHandler();
    public static final String ASTERIX_INSTANCE_NAME = "asterix";
    private static final String CC_IP_ADDRESS = "127.0.0.1";
    private static final int DEFAULT_HYRACKS_CC_CLIENT_PORT = 1098;
    private static final int zookeeperClientPort = 2900;
    private static final int zookeeperTestClientPort = 3945;
    private static IHyracksClientConnection hcc;

    public static void deinit() throws Exception {
        deleteInstance();
        stopZookeeper();
    }

    public static void init() throws Exception {
        managixHome = getManagixHome();
        System.setProperty("log4j.configuration",
                managixHome + File.separator + "conf" + File.separator + "log4j.properties");

        clusterConfigurationPath = managixHome + File.separator + "clusters" + File.separator + "local" + File.separator
                + "local.xml";

        InstallerDriver.setManagixHome(managixHome);

        String command = "configure";
        cmdHandler.processCommand(command.split(" "));
        command = "validate -c " + clusterConfigurationPath;
        cmdHandler.processCommand(command.split(" "));

        startZookeeper();
        Thread.sleep(2000);
        InstallerDriver.initConfig(true);
        createInstance();
        hcc = new HyracksConnection(CC_IP_ADDRESS, DEFAULT_HYRACKS_CC_CLIENT_PORT);
    }

    public static IHyracksClientConnection getHyracksConnection() {
        return hcc;
    }

    private static void startZookeeper() throws Exception {
        initZookeeperTestConfiguration(zookeeperClientPort);
        String script = managixHome + File.separator + "bin" + File.separator + "managix";

        // shutdown zookeeper if running
        String command = "shutdown";
        cmdHandler.processCommand(command.split(" "));

        Thread.sleep(2000);

        // start zookeeper
        initZookeeperTestConfiguration(zookeeperTestClientPort);
        ProcessBuilder pb2 = new ProcessBuilder(script, "describe");
        Map<String, String> env2 = pb2.environment();
        env2.put("MANAGIX_HOME", managixHome);
        pb2.start();

    }

    public static void createInstance() throws Exception {

        String command = null;
        AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService()
                .getAsterixInstance(ASTERIX_INSTANCE_NAME);
        if (instance != null) {
            transformIntoRequiredState(State.INACTIVE);
            command = "delete -n " + ASTERIX_INSTANCE_NAME;
            cmdHandler.processCommand(command.split(" "));
        }

        command = "create -n " + ASTERIX_INSTANCE_NAME + " " + "-c" + " " + clusterConfigurationPath;
        cmdHandler.processCommand(command.split(" "));

        instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(ASTERIX_INSTANCE_NAME);
        AsterixRuntimeState state = VerificationUtil.getAsterixRuntimeState(instance);
        assert (state.getFailedNCs().isEmpty() && state.isCcRunning());
    }

    private static void initZookeeperTestConfiguration(int port) throws JAXBException, FileNotFoundException {
        String installerConfPath = InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_CONF_XML;
        JAXBContext ctx = JAXBContext.newInstance(Configuration.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        Configuration configuration = (Configuration) unmarshaller.unmarshal(new File(installerConfPath));
        configuration.getZookeeper().setClientPort(new BigInteger("" + port));
        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(configuration, new FileOutputStream(installerConfPath));
    }

    public static void transformIntoRequiredState(AsterixInstance.State state) throws Exception {
        AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService()
                .getAsterixInstance(ASTERIX_INSTANCE_NAME);
        assert (instance != null);
        if (instance.getState().equals(state)) {
            return;
        }
        if (state.equals(AsterixInstance.State.UNUSABLE)) {
            throw new IllegalArgumentException("Invalid desired state");
        }

        String command = null;
        switch (instance.getState()) {
            case ACTIVE:
                command = "stop -n " + ASTERIX_INSTANCE_NAME;
                break;
            case INACTIVE:
                command = "start -n " + ASTERIX_INSTANCE_NAME;
                break;
            case UNUSABLE:
                command = "delete -n " + ASTERIX_INSTANCE_NAME;
                cmdHandler.processCommand(command.split(" "));
                throw new Exception("Cluster state was Unusable");
        }
        cmdHandler.processCommand(command.split(" "));
    }

    private static void stopZookeeper() throws IOException, JAXBException {
        String script = managixHome + File.separator + "bin" + File.separator + "managix";
        // shutdown zookeeper if running
        ProcessBuilder pb = new ProcessBuilder(script, "shutdown");
        Map<String, String> env = pb.environment();
        env.put("MANAGIX_HOME", managixHome);
        pb.start();
    }

    private static void deleteInstance() throws Exception {
        String command = null;
        AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService()
                .getAsterixInstance(ASTERIX_INSTANCE_NAME);

        if (instance == null) {
            return;
        } else {
            transformIntoRequiredState(State.INACTIVE);
            command = "delete -n " + ASTERIX_INSTANCE_NAME;
            cmdHandler.processCommand(command.split(" "));
        }
        instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(ASTERIX_INSTANCE_NAME);
        assert (instance == null);
    }

    public static String getManagixHome() {
        File asterixProjectDir = new File(System.getProperty("user.dir"));

        File installerTargetDir = new File(asterixProjectDir, "target");
        String managixHomeDirName = installerTargetDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory() && name.startsWith("asterix-installer")
                        && name.endsWith("binary-assembly");
            }

        })[0];
        return new File(installerTargetDir, managixHomeDirName).getAbsolutePath();
    }

    public static void installLibrary(String libraryName, String libraryDataverse, String libraryPath)
            throws Exception {
        transformIntoRequiredState(State.INACTIVE);
        String command = "install -n " + ASTERIX_INSTANCE_NAME + " -d " + libraryDataverse + " -l " + libraryName
                + " -p " + libraryPath;
        cmdHandler.processCommand(command.split(" "));
    }

    public static void uninstallLibrary(String dataverseName, String libraryName) throws Exception {
        transformIntoRequiredState(State.INACTIVE);
        String command = "uninstall -n " + ASTERIX_INSTANCE_NAME + " -d " + dataverseName + " -l " + "libraryName";
        cmdHandler.processCommand(command.split(" "));
    }

    public static void executeCommand(String command) throws Exception {
        cmdHandler.processCommand(command.trim().split(" "));
    }

}
