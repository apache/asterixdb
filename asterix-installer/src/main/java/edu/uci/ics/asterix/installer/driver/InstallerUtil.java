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
package edu.uci.ics.asterix.installer.driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import edu.uci.ics.asterix.common.configuration.AsterixConfiguration;
import edu.uci.ics.asterix.common.configuration.Coredump;
import edu.uci.ics.asterix.common.configuration.Store;
import edu.uci.ics.asterix.common.configuration.TransactionLogDir;
import edu.uci.ics.asterix.event.driver.EventDriver;
import edu.uci.ics.asterix.event.management.EventUtil;
import edu.uci.ics.asterix.event.management.EventrixClient;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Env;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.cluster.Property;
import edu.uci.ics.asterix.installer.error.InstallerException;
import edu.uci.ics.asterix.installer.error.OutputHandler;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class InstallerUtil {

    private static final String DEFAULT_ASTERIX_CONFIGURATION_PATH = "conf" + File.separator
            + "asterix-configuration.xml";

    public static final String TXN_LOG_DIR = "txnLogs";
    public static final String TXN_LOG_DIR_KEY_SUFFIX = "txnLogDir";
    public static final String ASTERIX_CONFIGURATION_FILE = "asterix-configuration.xml";
    public static final String TXN_LOG_CONFIGURATION_FILE = "log.properties";
    public static final int CLUSTER_NET_PORT_DEFAULT = 1098;
    public static final int CLIENT_NET_PORT_DEFAULT = 1099;
    public static final int HTTP_PORT_DEFAULT = 8888;
    public static final int WEB_INTERFACE_PORT_DEFAULT = 19001;

    public static AsterixInstance createAsterixInstance(String asterixInstanceName, Cluster cluster,
            AsterixConfiguration asterixConfiguration) throws FileNotFoundException, IOException {
        Node metadataNode = getMetadataNode(cluster);
        String asterixZipName = InstallerDriver.getAsterixZip().substring(
                InstallerDriver.getAsterixZip().lastIndexOf(File.separator) + 1);
        String asterixVersion = asterixZipName.substring("asterix-server-".length(),
                asterixZipName.indexOf("-binary-assembly"));
        AsterixInstance instance = new AsterixInstance(asterixInstanceName, cluster, asterixConfiguration,
                metadataNode.getId(), asterixVersion);
        return instance;
    }

    public static void createAsterixZip(AsterixInstance asterixInstance) throws IOException, InterruptedException,
            JAXBException, InstallerException {

        String modifiedZipPath = injectAsterixPropertyFile(InstallerDriver.getAsterixZip(), asterixInstance);
        injectAsterixLogPropertyFile(modifiedZipPath, asterixInstance);
    }

    public static void createClusterProperties(Cluster cluster, AsterixConfiguration asterixConfiguration) {
        List<Property> clusterProperties = null;
        if (cluster.getEnv() != null && cluster.getEnv().getProperty() != null) {
            clusterProperties = cluster.getEnv().getProperty();
            clusterProperties.clear();
        } else {
            clusterProperties = new ArrayList<Property>();
        }
        for (edu.uci.ics.asterix.common.configuration.Property property : asterixConfiguration.getProperty()) {
            if (property.getName().equalsIgnoreCase(EventUtil.CC_JAVA_OPTS)) {
                clusterProperties.add(new Property(EventUtil.CC_JAVA_OPTS, property.getValue()));
            } else if (property.getName().equalsIgnoreCase(EventUtil.NC_JAVA_OPTS)) {
                clusterProperties.add(new Property(EventUtil.NC_JAVA_OPTS, property.getValue()));
            }
        }
        clusterProperties.add(new Property("ASTERIX_HOME", cluster.getWorkingDir().getDir() + File.separator
                + "asterix"));
        clusterProperties.add(new Property("LOG_DIR", cluster.getLogDir()));
        clusterProperties.add(new Property("JAVA_HOME", cluster.getJavaHome()));
        clusterProperties.add(new Property("WORKING_DIR", cluster.getWorkingDir().getDir()));
        clusterProperties.add(new Property("CLIENT_NET_IP", cluster.getMasterNode().getClientIp()));
        clusterProperties.add(new Property("CLUSTER_NET_IP", cluster.getMasterNode().getClusterIp()));

        int clusterNetPort = cluster.getMasterNode().getClusterPort() != null ? cluster.getMasterNode()
                .getClusterPort().intValue() : CLUSTER_NET_PORT_DEFAULT;
        int clientNetPort = cluster.getMasterNode().getClientPort() != null ? cluster.getMasterNode().getClientPort()
                .intValue() : CLIENT_NET_PORT_DEFAULT;
        int httpPort = cluster.getMasterNode().getHttpPort() != null ? cluster.getMasterNode().getHttpPort().intValue()
                : HTTP_PORT_DEFAULT;

        clusterProperties.add(new Property("CLIENT_NET_PORT", "" + clientNetPort));
        clusterProperties.add(new Property("CLUSTER_NET_PORT", "" + clusterNetPort));
        clusterProperties.add(new Property("HTTP_PORT", "" + httpPort));

        cluster.setEnv(new Env(clusterProperties));
    }

    private static String injectAsterixPropertyFile(String origZipFile, AsterixInstance asterixInstance)
            throws IOException, JAXBException {
        writeAsterixConfigurationFile(asterixInstance);
        String asterixInstanceDir = InstallerDriver.getAsterixDir() + File.separator + asterixInstance.getName();
        unzip(origZipFile, asterixInstanceDir);
        File sourceJar = new File(asterixInstanceDir + File.separator + "lib" + File.separator + "asterix-app-"
                + asterixInstance.getAsterixVersion() + ".jar");
        File replacementFile = new File(asterixInstanceDir + File.separator + ASTERIX_CONFIGURATION_FILE);
        replaceInJar(sourceJar, ASTERIX_CONFIGURATION_FILE, replacementFile);
        new File(asterixInstanceDir + File.separator + ASTERIX_CONFIGURATION_FILE).delete();
        String asterixZipName = InstallerDriver.getAsterixZip().substring(
                InstallerDriver.getAsterixZip().lastIndexOf(File.separator) + 1);
        zipDir(new File(asterixInstanceDir), new File(asterixInstanceDir + File.separator + asterixZipName));
        return asterixInstanceDir + File.separator + asterixZipName;
    }

    private static String injectAsterixLogPropertyFile(String origZipFile, AsterixInstance asterixInstance)
            throws IOException, InstallerException {
        String asterixInstanceDir = InstallerDriver.getAsterixDir() + File.separator + asterixInstance.getName();
        unzip(origZipFile, asterixInstanceDir);
        File sourceJar1 = new File(asterixInstanceDir + File.separator + "lib" + File.separator + "asterix-app-"
                + asterixInstance.getAsterixVersion() + ".jar");
        Properties txnLogProperties = new Properties();
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[] { sourceJar1.toURI().toURL() });
        InputStream in = urlClassLoader.getResourceAsStream(TXN_LOG_CONFIGURATION_FILE);
        if (in != null) {
            txnLogProperties.load(in);
        }

        writeAsterixLogConfigurationFile(asterixInstance, txnLogProperties);

        File sourceJar2 = new File(asterixInstanceDir + File.separator + "lib" + File.separator + "asterix-app-"
                + asterixInstance.getAsterixVersion() + ".jar");
        File replacementFile = new File(asterixInstanceDir + File.separator + "log.properties");
        replaceInJar(sourceJar2, TXN_LOG_CONFIGURATION_FILE, replacementFile);

        new File(asterixInstanceDir + File.separator + "log.properties").delete();
        String asterixZipName = InstallerDriver.getAsterixZip().substring(
                InstallerDriver.getAsterixZip().lastIndexOf(File.separator) + 1);
        zipDir(new File(asterixInstanceDir), new File(asterixInstanceDir + File.separator + asterixZipName));
        return asterixInstanceDir + File.separator + asterixZipName;
    }

    public static void addLibraryToAsterixZip(AsterixInstance asterixInstance, String dataverseName,
            String libraryName, String libraryPath) throws IOException {
        File instanceDir = new File(InstallerDriver.getAsterixDir() + File.separator + asterixInstance.getName());
        if (!instanceDir.exists()) {
            instanceDir.mkdirs();
        }
        String asterixZipName = InstallerDriver.getAsterixZip().substring(
                InstallerDriver.getAsterixZip().lastIndexOf(File.separator) + 1);

        String sourceZip = instanceDir.getAbsolutePath() + File.separator + asterixZipName;
        unzip(sourceZip, instanceDir.getAbsolutePath());
        File libraryPathInZip = new File(instanceDir.getAbsolutePath() + File.separator + "external" + File.separator
                + "library" + dataverseName + File.separator + "to-add" + File.separator + libraryName);
        libraryPathInZip.mkdirs();
        Runtime.getRuntime().exec("cp" + " " + libraryPath + " " + libraryPathInZip.getAbsolutePath());
        Runtime.getRuntime().exec("rm " + sourceZip);
        String destZip = InstallerDriver.getAsterixDir() + File.separator + asterixInstance.getName() + File.separator
                + asterixZipName;
        zipDir(instanceDir, new File(destZip));
        Runtime.getRuntime().exec("mv" + " " + destZip + " " + sourceZip);
    }

    private static Node getMetadataNode(Cluster cluster) {
        Random random = new Random();
        int nNodes = cluster.getNode().size();
        return cluster.getNode().get(random.nextInt(nNodes));
    }

    public static String getNodeDirectories(String asterixInstanceName, Node node, Cluster cluster) {
        String storeDataSubDir = asterixInstanceName + File.separator + "data" + File.separator;
        String[] storeDirs = null;
        StringBuffer nodeDataStore = new StringBuffer();
        String storeDirValue = node.getStore();
        if (storeDirValue == null) {
            storeDirValue = cluster.getStore();
            if (storeDirValue == null) {
                throw new IllegalStateException(" Store not defined for node " + node.getId());
            }
            storeDataSubDir = node.getId() + File.separator + storeDataSubDir;
        }

        storeDirs = storeDirValue.split(",");
        for (String ns : storeDirs) {
            nodeDataStore.append(ns + File.separator + storeDataSubDir.trim());
            nodeDataStore.append(",");
        }
        nodeDataStore.deleteCharAt(nodeDataStore.length() - 1);
        return nodeDataStore.toString();
    }

    private static void writeAsterixConfigurationFile(AsterixInstance asterixInstance) throws IOException,
            JAXBException {
        String asterixInstanceName = asterixInstance.getName();
        Cluster cluster = asterixInstance.getCluster();
        String metadataNodeId = asterixInstance.getMetadataNodeId();

        AsterixConfiguration configuration = asterixInstance.getAsterixConfiguration();
        configuration.setMetadataNode(asterixInstanceName + "_" + metadataNodeId);

        String storeDir = null;
        List<Store> stores = new ArrayList<Store>();
        for (Node node : cluster.getNode()) {
            storeDir = node.getStore() == null ? cluster.getStore() : node.getStore();
            stores.add(new Store(asterixInstanceName + "_" + node.getId(), storeDir));
        }
        configuration.setStore(stores);

        List<Coredump> coredump = new ArrayList<Coredump>();
        String coredumpDir = null;
        List<TransactionLogDir> txnLogDirs = new ArrayList<TransactionLogDir>();
        String txnLogDir = null;
        for (Node node : cluster.getNode()) {
            coredumpDir = node.getLogDir() == null ? cluster.getLogDir() : node.getLogDir();
            coredump.add(new Coredump(asterixInstanceName + "_" + node.getId(), coredumpDir + File.separator
                    + asterixInstanceName + "_" + node.getId()));

            txnLogDir = node.getTxnLogDir() == null ? cluster.getTxnLogDir() : node.getTxnLogDir();
            txnLogDirs.add(new TransactionLogDir(asterixInstanceName + "_" + node.getId(), txnLogDir));
        }
        configuration.setCoredump(coredump);
        configuration.setTransactionLogDir(txnLogDirs);

        File asterixConfDir = new File(InstallerDriver.getAsterixDir() + File.separator + asterixInstanceName);
        asterixConfDir.mkdirs();

        JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(configuration, new FileOutputStream(asterixConfDir + File.separator
                + ASTERIX_CONFIGURATION_FILE));
    }

    private static void writeAsterixLogConfigurationFile(AsterixInstance asterixInstance, Properties logProperties)
            throws IOException, InstallerException {
        String asterixInstanceName = asterixInstance.getName();
        Cluster cluster = asterixInstance.getCluster();
        StringBuffer conf = new StringBuffer();
        for (Map.Entry<Object, Object> p : logProperties.entrySet()) {
            conf.append(p.getKey() + "=" + p.getValue() + "\n");
        }

        for (Node node : cluster.getNode()) {
            String txnLogDir = node.getTxnLogDir() == null ? cluster.getTxnLogDir() : node.getTxnLogDir();
            if (txnLogDir == null) {
                throw new InstallerException("Transaction log directory (txn_log_dir) not configured for node: "
                        + node.getId());
            }
            conf.append(asterixInstanceName + "_" + node.getId() + "." + TXN_LOG_DIR_KEY_SUFFIX + "=" + txnLogDir
                    + "\n");
        }
        List<edu.uci.ics.asterix.common.configuration.Property> properties = asterixInstance.getAsterixConfiguration()
                .getProperty();
        for (edu.uci.ics.asterix.common.configuration.Property p : properties) {
            if (p.getName().trim().toLowerCase().contains("log")) {
                conf.append(p.getValue() + "=" + p.getValue());
            }
        }
        dumpToFile(InstallerDriver.getAsterixDir() + File.separator + asterixInstanceName + File.separator
                + "log.properties", conf.toString());

    }

    public static AsterixConfiguration getAsterixConfiguration(String asterixConf) throws FileNotFoundException,
            IOException, JAXBException {
        if (asterixConf == null) {
            asterixConf = InstallerDriver.getManagixHome() + File.separator + DEFAULT_ASTERIX_CONFIGURATION_PATH;
        }
        File file = new File(asterixConf);
        JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        AsterixConfiguration asterixConfiguration = (AsterixConfiguration) unmarshaller.unmarshal(file);
        return asterixConfiguration;
    }

}
