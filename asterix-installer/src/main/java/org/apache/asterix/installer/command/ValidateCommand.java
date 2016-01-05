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
package org.apache.asterix.installer.command;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.event.management.EventUtil;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.MasterNode;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.installer.driver.InstallerDriver;
import org.apache.asterix.installer.schema.conf.Configuration;
import org.apache.asterix.installer.schema.conf.Zookeeper;
import org.kohsuke.args4j.Option;

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
        File home = new File(InstallerDriver.getManagixHome());
        if (!home.exists()) {
            valid = false;
            LOGGER.fatal(InstallerDriver.ENV_MANAGIX_HOME + ": " + home.getAbsolutePath() + " does not exist!" + ERROR);
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
            valid = valid & validateClusterProperties(cluster);

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

            valid = valid & validateReplicationProperties(cluster);
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
        String output = AsterixEventServiceUtil.executeLocalScript(script, args);
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

    private boolean validateClusterProperties(Cluster cluster) {
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

        if (cluster.getStore() == null || cluster.getStore().length() == 0) {
            LOGGER.fatal("store not defined at cluster" + ERROR);
            return false;
        }
        return true;
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

        if (node.getTxnLogDir() == null || node.getTxnLogDir().length() == 0) {
            if (cluster.getTxnLogDir() == null || cluster.getTxnLogDir().length() == 0) {
                valid = false;
                LOGGER.fatal("txn_log_dir not defined at cluster/node level for node: " + node.getId() + ERROR);
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

    private boolean validateReplicationProperties(Cluster cluster) {
        boolean valid = true;

        //if replication is disabled, no need to validate the settings
        if (cluster.getDataReplication() != null && cluster.getDataReplication().isEnabled()) {

            if (cluster.getDataReplication().getReplicationFactor() == null) {
                if (cluster.getNode().size() >= 3) {
                    LOGGER.warn("Replication factor not defined. Using default value (3) " + WARNING);

                } else {
                    valid = false;
                    LOGGER.fatal("Replication factor not defined for data repliaction. " + ERROR);
                }

            }

            //replication factor = 1 means no replication
            if (cluster.getDataReplication().getReplicationFactor().intValue() == 1) {
                LOGGER.warn("Replication factor is set to 1. Disabling data replication" + WARNING);
                return true;
            }

            if (cluster.getDataReplication().getReplicationFactor().intValue() > cluster.getNode().size()) {
                LOGGER.fatal("Replication factor = " + cluster.getDataReplication().getReplicationFactor().intValue()
                        + "  requires at least " + cluster.getDataReplication().getReplicationFactor().intValue()
                        + " nodes in the cluster" + ERROR);
                valid = false;
            }

            if (cluster.getDataReplication().getReplicationStore() == null
                    || cluster.getDataReplication().getReplicationStore().length() == 0) {
                valid = false;
                LOGGER.fatal("Replication store not defined. " + ERROR);
            }

            if (cluster.getDataReplication().getReplicationPort() == null
                    || cluster.getDataReplication().getReplicationPort().toString().length() == 0) {
                valid = false;
                LOGGER.fatal("Replication data port not defined for data repliaction. " + ERROR);
            }

            if (cluster.getDataReplication().getReplicationTimeOut() == null
                    || (cluster.getDataReplication().getReplicationTimeOut().intValue() + "").length() == 0) {
                LOGGER.warn("Replication maximum wait time not defined. Using default value (60 seconds) " + WARNING);
            }

            //validate all nodes have the same number of io devices
            int numOfIODevices = 0;
            Set<Integer> ioDevicesCount = new HashSet<Integer>();
            for (int i = 0; i < cluster.getNode().size(); i++) {
                Node node = cluster.getNode().get(i);

                if (node.getIodevices() != null) {
                    numOfIODevices = node.getIodevices().length() - node.getIodevices().replace(",", "").length();
                } else {
                    numOfIODevices = cluster.getIodevices().length() - cluster.getIodevices().replace(",", "").length();
                }

                ioDevicesCount.add(numOfIODevices);

                if (ioDevicesCount.size() > 1) {
                    valid = false;
                    LOGGER.fatal("Replication requires all nodes to have the same number of IO devices." + ERROR);
                    break;
                }
            }

        }

        return valid;
    }

}

class ValidateConfig extends CommandConfig {

    @Option(name = "-c", required = false, usage = "Path to the cluster configuration xml")
    public String cluster;

}
