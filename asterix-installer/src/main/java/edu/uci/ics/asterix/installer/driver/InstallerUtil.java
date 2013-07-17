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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.io.IOUtils;

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
            asterixConf = InstallerDriver.getManagixHome() + File.separator
                    + InstallerDriver.DEFAULT_ASTERIX_CONFIGURATION_PATH;
        }
        File file = new File(asterixConf);
        JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        AsterixConfiguration asterixConfiguration = (AsterixConfiguration) unmarshaller.unmarshal(file);
        return asterixConfiguration;
    }

    public static void unzip(String sourceFile, String destDir) throws IOException {
        BufferedOutputStream dest = null;
        FileInputStream fis = new FileInputStream(sourceFile);
        ZipInputStream zis = new ZipInputStream(new BufferedInputStream(fis));
        ZipEntry entry = null;

        int BUFFER_SIZE = 4096;
        while ((entry = zis.getNextEntry()) != null) {
            String dst = destDir + File.separator + entry.getName();
            if (entry.isDirectory()) {
                createDir(destDir, entry);
                continue;
            }
            int count;
            byte data[] = new byte[BUFFER_SIZE];

            // write the file to the disk
            FileOutputStream fos = new FileOutputStream(dst);
            dest = new BufferedOutputStream(fos, BUFFER_SIZE);
            while ((count = zis.read(data, 0, BUFFER_SIZE)) != -1) {
                dest.write(data, 0, count);
            }
            // close the output streams
            dest.flush();
            dest.close();
        }

        zis.close();
    }

    public static void zipDir(File sourceDir, File destFile) throws IOException {
        FileOutputStream fos = new FileOutputStream(destFile);
        ZipOutputStream zos = new ZipOutputStream(fos);
        zipDir(sourceDir, destFile, zos);
        zos.close();
    }

    private static void zipDir(File sourceDir, final File destFile, ZipOutputStream zos) throws IOException {
        File[] dirList = sourceDir.listFiles(new FileFilter() {
            public boolean accept(File f) {
                return !f.getName().endsWith(destFile.getName());
            }
        });
        for (int i = 0; i < dirList.length; i++) {
            File f = dirList[i];
            if (f.isDirectory()) {
                zipDir(f, destFile, zos);
            } else {
                int bytesIn = 0;
                byte[] readBuffer = new byte[2156];
                FileInputStream fis = new FileInputStream(f);
                ZipEntry entry = new ZipEntry(sourceDir.getName() + File.separator + f.getName());
                zos.putNextEntry(entry);
                while ((bytesIn = fis.read(readBuffer)) != -1) {
                    zos.write(readBuffer, 0, bytesIn);
                }
                fis.close();
            }
        }
    }

    private static void replaceInJar(File sourceJar, String origFile, File replacementFile) throws IOException {
        File destJar = new File(sourceJar.getAbsolutePath() + ".modified");
        InputStream jarIs = null;
        FileInputStream fis = new FileInputStream(replacementFile);
        JarFile sourceJarFile = new JarFile(sourceJar);
        Enumeration<JarEntry> entries = sourceJarFile.entries();
        JarOutputStream jos = new JarOutputStream(new FileOutputStream(destJar));
        byte[] buffer = new byte[2048];
        int read;
        while (entries.hasMoreElements()) {
            JarEntry entry = (JarEntry) entries.nextElement();
            String name = entry.getName();
            if (name.equals(origFile)) {
                continue;
            }
            jarIs = sourceJarFile.getInputStream(entry);
            jos.putNextEntry(entry);
            while ((read = jarIs.read(buffer)) != -1) {
                jos.write(buffer, 0, read);
            }
        }
        JarEntry entry = new JarEntry(origFile);
        jos.putNextEntry(entry);
        while ((read = fis.read(buffer)) != -1) {
            jos.write(buffer, 0, read);
        }
        fis.close();
        jos.close();
        jarIs.close();
        sourceJar.delete();
        destJar.renameTo(sourceJar);
        sourceJar.setExecutable(true);
    }

    public static void dumpToFile(String dest, String content) throws IOException {
        FileWriter writer = new FileWriter(dest);
        writer.write(content);
        writer.close();
    }

    private static void createDir(String destDirectory, ZipEntry entry) {
        String name = entry.getName();
        int index = name.lastIndexOf(File.separator);
        String dirSequence = name.substring(0, index);
        File newDirs = new File(destDirectory + File.separator + dirSequence);
        newDirs.mkdirs();
    }

    public static AsterixInstance validateAsterixInstanceExists(String name, State... permissibleStates)
            throws Exception {
        AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(name);
        if (instance == null) {
            throw new InstallerException("Asterix instance by name " + name + " does not exist.");
        }
        boolean valid = false;
        for (State state : permissibleStates) {
            if (state.equals(instance.getState())) {
                valid = true;
                break;
            }
        }
        if (!valid) {
            throw new InstallerException("Asterix instance by the name " + name + " is in " + instance.getState()
                    + " state ");
        }
        return instance;
    }

    public static void validateAsterixInstanceNotExists(String name) throws Exception {
        AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(name);
        if (instance != null) {
            throw new InstallerException("Asterix instance by name " + name + " already exists.");
        }
    }

    public static void evaluateConflictWithOtherInstances(AsterixInstance instance) throws Exception {
        List<AsterixInstance> existingInstances = ServiceProvider.INSTANCE.getLookupService().getAsterixInstances();
        List<String> usedIps = new ArrayList<String>();
        String masterIp = instance.getCluster().getMasterNode().getClusterIp();
        for (Node node : instance.getCluster().getNode()) {
            usedIps.add(node.getClusterIp());
        }
        usedIps.add(instance.getCluster().getMasterNode().getClusterIp());
        boolean conflictFound = false;
        AsterixInstance conflictingInstance = null;
        for (AsterixInstance existing : existingInstances) {
            if (!existing.getState().equals(State.INACTIVE)) {
                conflictFound = existing.getCluster().getMasterNode().getClusterIp().equals(masterIp);
                if (conflictFound) {
                    conflictingInstance = existing;
                    break;
                }
                for (Node n : existing.getCluster().getNode()) {
                    if (usedIps.contains(n.getClusterIp())) {
                        conflictFound = true;
                        conflictingInstance = existing;
                        break;
                    }
                }
            }
        }
        if (conflictFound) {
            throw new Exception("Cluster definition conflicts with an existing instance of Asterix: "
                    + conflictingInstance.getName());
        }
    }

    public static void deleteDirectory(String path) throws IOException {
        Runtime.getRuntime().exec("rm -rf " + path);
    }

    public static String executeLocalScript(String path, List<String> args) throws Exception {
        List<String> pargs = new ArrayList<String>();
        pargs.add("/bin/bash");
        pargs.add(path);
        if (args != null) {
            pargs.addAll(args);
        }
        ProcessBuilder pb = new ProcessBuilder(pargs);
        pb.environment().putAll(EventDriver.getEnvironment());
        pb.environment().put("IP_LOCATION", EventDriver.CLIENT_NODE.getClusterIp());
        Process p = pb.start();
        BufferedInputStream bis = new BufferedInputStream(p.getInputStream());
        StringWriter writer = new StringWriter();
        IOUtils.copy(bis, writer, "UTF-8");
        return writer.toString();
    }

    public static EventrixClient getEventrixClient(Cluster cluster) throws Exception {
        return new EventrixClient(
                InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_EVENT_DIR, cluster, false,
                OutputHandler.INSTANCE);
    }

}
