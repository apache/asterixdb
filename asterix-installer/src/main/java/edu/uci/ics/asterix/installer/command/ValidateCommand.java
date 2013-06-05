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
package edu.uci.ics.asterix.installer.command;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.management.EventUtil;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.MasterNode;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;
import edu.uci.ics.asterix.installer.schema.conf.Zookeeper;

public class ValidateCommand extends AbstractCommand {

    private static final String OK = " [" + "OK" + "]";
    private static final String ERROR = " [" + "ERROR" + "]";
    private static final String WARNING = " [" + "WARNING" + "]";

    @Override
    protected void execCommand() throws Exception {
        ValidateConfig vConfig = (ValidateConfig) config;
        logValidationResult("Environment", validateEnvironment());
        if (((ValidateConfig) config).cluster != null) {
            logValidationResult("Cluster configuration", validateCluster(vConfig.cluster));
        } else {
            logValidationResult("Managix Configuration", validateConfiguration());
        }
    }

    private void logValidationResult(String prefix, boolean isValid) {
        if (!isValid) {
            LOGGER.fatal(prefix + ERROR);
        } else {
            LOGGER.info(prefix + OK);
        }
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new ValidateConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nValidate the installer's configuration or a cluster configuration" + "\nUsage"
                + "\nFor validating the installer configuration" + "\nuse managix validate"
                + "\n\nFor validating a cluster configuration"
                + "\nuse managix validate -c <path to the cluster configuration file>";
    }

    public boolean validateEnvironment() throws Exception {
        boolean valid = true;
        String managixHome = System.getenv(InstallerDriver.ENV_MANAGIX_HOME);
        if (managixHome == null) {
            valid = false;
            LOGGER.fatal(InstallerDriver.ENV_MANAGIX_HOME + " not set " + ERROR);
        } else {
            File home = new File(managixHome);
            if (!home.exists()) {
                valid = false;
                LOGGER.fatal(InstallerDriver.ENV_MANAGIX_HOME + ": " + home.getAbsolutePath() + " does not exist!"
                        + ERROR);
            }
        }
        return valid;

    }

    public boolean validateCluster(String clusterPath) throws Exception {
        boolean valid = true;
        Cluster cluster = null;
        File f = new File(clusterPath);
        List<String> ipAddresses = new ArrayList<String>();
        if (!f.exists() || !f.isFile()) {
            LOGGER.error(" Invalid path " + f.getAbsolutePath() + ERROR);
            valid = false;
        } else {
            cluster = EventUtil.getCluster(clusterPath);
            validateClusterProperties(cluster);

            Set<String> servers = new HashSet<String>();
            Set<String> serverIds = new HashSet<String>();
            servers.add(cluster.getMasterNode().getClusterIp());
            serverIds.add(cluster.getMasterNode().getId());

            MasterNode masterNode = cluster.getMasterNode();
            Node master = new Node(masterNode.getId(), masterNode.getClusterIp(), masterNode.getJavaHome(),
                    masterNode.getLogDir(), null, null, null);
            ipAddresses.add(masterNode.getClusterIp());

            valid = valid & validateNodeConfiguration(master, cluster);

            for (Node node : cluster.getNode()) {
                servers.add(node.getClusterIp());
                if (serverIds.contains(node.getId())) {
                    valid = false;
                    LOGGER.error("Duplicate node id :" + node.getId() + ERROR);
                } else {
                    valid = valid & validateNodeConfiguration(node, cluster);
                    if (!ipAddresses.contains(node.getClusterIp())) {
                        ipAddresses.add(node.getClusterIp());
                    }
                }
            }
        }

        if (valid) {
            String username = cluster.getUsername();
            if (username == null) {
                username = System.getProperty("user.name");
            }
            valid = checkPasswordLessSSHLogin(username, ipAddresses);
        }
        return valid;
    }

    private boolean checkPasswordLessSSHLogin(String username, List<String> ipAddresses) throws Exception {
        String script = InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_INTERNAL_DIR
                + File.separator + "scripts" + File.separator + "validate_ssh.sh";
        List<String> args = ipAddresses;
        args.add(0, username);
        String output = InstallerUtil.executeLocalScript(script, args);
        ipAddresses.remove(0);
        for (String line : output.split("\n")) {
            ipAddresses.remove(line);
        }
        if (ipAddresses.size() > 0) {
            LOGGER.error(" Password-less SSH (from user account: " + username + " )"
                    + " not configured for the following hosts");
            for (String failedIp : ipAddresses) {
                System.out.println(failedIp);
            }
            return false;
        }
        return true;
    }

    private void validateClusterProperties(Cluster cluster) {
        List<String> tempDirs = new ArrayList<String>();
        if (cluster.getLogDir() != null && checkTemporaryPath(cluster.getLogDir())) {
            tempDirs.add("Log directory: " + cluster.getLogDir());
        }
        if (cluster.getIodevices() != null && checkTemporaryPath(cluster.getIodevices())) {
            tempDirs.add("IO Device: " + cluster.getIodevices());
        }

        if (tempDirs.size() > 0) {
            StringBuffer msg = new StringBuffer();
            msg.append("The following paths are subject to be cleaned up by OS");
            for (String tempDir : tempDirs) {
                msg.append("\n" + tempDir + WARNING);
            }
            LOGGER.warn(msg);
        }

    }

    private boolean validateNodeConfiguration(Node node, Cluster cluster) {
        boolean valid = true;
        if (node.getJavaHome() == null || node.getJavaHome().length() == 0) {
            if (cluster.getJavaHome() == null || cluster.getJavaHome().length() == 0) {
                valid = false;
                LOGGER.fatal("java_home not defined at cluster/node level for node: " + node.getId() + ERROR);
            }
        }

        if (node.getLogDir() == null || node.getLogDir().length() == 0) {
            if (cluster.getLogDir() == null || cluster.getLogDir().length() == 0) {
                valid = false;
                LOGGER.fatal("log_dir not defined at cluster/node level for node: " + node.getId() + ERROR);
            }
        }

        if (node.getStore() == null || node.getStore().length() == 0) {
            if (!cluster.getMasterNode().getId().equals(node.getId())
                    && (cluster.getStore() == null || cluster.getStore().length() == 0)) {
                valid = false;
                LOGGER.fatal("store not defined at cluster/node level for node: " + node.getId() + ERROR);
            }
        }

        if (node.getIodevices() == null || node.getIodevices().length() == 0) {
            if (!cluster.getMasterNode().getId().equals(node.getId())
                    && (cluster.getIodevices() == null || cluster.getIodevices().length() == 0)) {
                valid = false;
                LOGGER.fatal("iodevice(s) not defined at cluster/node level for node: " + node.getId() + ERROR);
            }
        }

        return valid;
    }

    private boolean checkTemporaryPath(String logdir) {
        return logdir.startsWith(System.getProperty("java.io.tmpdir"));
    }

    public boolean validateConfiguration() throws Exception {
        String managixHome = System.getenv(InstallerDriver.ENV_MANAGIX_HOME);
        File configFile = new File(managixHome + File.separator + InstallerDriver.MANAGIX_CONF_XML);
        JAXBContext configCtx = JAXBContext.newInstance(Configuration.class);
        Unmarshaller unmarshaller = configCtx.createUnmarshaller();
        Configuration conf = (Configuration) unmarshaller.unmarshal(configFile);
        return validateZookeeperConfiguration(conf);
    }

    private boolean validateZookeeperConfiguration(Configuration conf) throws Exception {
        boolean valid = true;
        Zookeeper zk = conf.getZookeeper();

        if (zk.getHomeDir() == null || zk.getHomeDir().length() == 0) {
            valid = false;
            LOGGER.fatal("Zookeeper home dir not configured" + ERROR);
        } else if (checkTemporaryPath(zk.getHomeDir())) {
            LOGGER.warn("Zookeeper home dir is subject to be cleaned up by OS" + WARNING);
        }

        if (zk.getServers().getServer() == null || zk.getServers().getServer().isEmpty()) {
            valid = false;
            LOGGER.fatal("Zookeeper servers not configured" + ERROR);
        }

        if (zk.getServers().getJavaHome() == null || zk.getServers().getJavaHome().length() == 0) {
            valid = false;
            LOGGER.fatal("Java home not set for Zookeeper server in " + InstallerDriver.getManagixHome()
                    + File.separator + InstallerDriver.MANAGIX_CONF_XML);
        }

        if (valid) {
            valid = valid & checkPasswordLessSSHLogin(System.getProperty("user.name"), zk.getServers().getServer());
        }

        return valid;
    }

}

class ValidateConfig extends CommandConfig {

    @Option(name = "-c", required = false, usage = "Path to the cluster configuration xml")
    public String cluster;

}
