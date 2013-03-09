/*
 * Copyright 2009-2012 by The Regents of the University of California
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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.MasterNode;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;
import edu.uci.ics.asterix.installer.schema.conf.Zookeeper;

public class ValidateCommand extends AbstractCommand {

    private static final String OK = " [" + "\u2713" + "]";
    private static final String ERROR = " [" + "x" + "]";
    private static final String WARNING = " [" + "!" + "]";

    @Override
    protected void execCommand() throws Exception {
        ValidateConfig vConfig = (ValidateConfig) config;
        logValidationResult("Enviornment", validateEnvironment());
        if (((ValidateConfig) config).cluster != null) {
            logValidationResult("Cluster configuration", validateCluster(vConfig.cluster));
        } else {
            logValidationResult("Installer Configuration", validateConfiguration());
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
                + "\nFor validating the installer configuration" + "\n use $ managix validate"
                + "\n\nFor validating a cluster configuration"
                + "\n$ use managix validate -c <path to the cluster configuration file>";
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
        File f = new File(clusterPath);
        if (!f.exists() || !f.isFile()) {
            LOGGER.error(" Invalid path " + f.getAbsolutePath() + ERROR);
            valid = false;
        } else {
            JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
            Unmarshaller unmarshaller = ctx.createUnmarshaller();
            Cluster cluster = (Cluster) unmarshaller.unmarshal(new File(clusterPath));
            validateClusterProperties(cluster);

            Set<String> servers = new HashSet<String>();
            Set<String> serverIds = new HashSet<String>();
            servers.add(cluster.getMasterNode().getIp());
            serverIds.add(cluster.getMasterNode().getId());

            MasterNode masterNode = cluster.getMasterNode();
            Node master = new Node(masterNode.getId(), masterNode.getIp(), masterNode.getRam(),
                    masterNode.getJavaHome(), masterNode.getLogdir(), null, masterNode.getDebug());

            valid = valid & validateNodeConfiguration(master, cluster);

            for (Node node : cluster.getNode()) {
                servers.add(node.getIp());
                if (serverIds.contains(node.getId())) {
                    valid = false;
                    LOGGER.error("Duplicate node id :" + node.getId() + ERROR);
                } else {
                    valid = valid & validateNodeConfiguration(node, cluster);
                }
            }
        }
        return valid;
    }

    private void validateClusterProperties(Cluster cluster) {
        List<String> tempDirs = new ArrayList<String>();
        if (cluster.getLogdir() != null && checkTemporaryPath(cluster.getLogdir())) {
            tempDirs.add("Log directory: " + cluster.getLogdir());
        }
        if (cluster.getStore() != null && checkTemporaryPath(cluster.getStore())) {
            tempDirs.add("Store directory: " + cluster.getStore());
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
        valid = checkNodeReachability(node.getIp());
        if (node.getJavaHome() == null || node.getJavaHome().length() == 0) {
            if (cluster.getJavaHome() == null || cluster.getJavaHome().length() == 0) {
                valid = false;
                LOGGER.fatal("java_home not defined at cluster/node level for node: " + node.getId() + ERROR);
            }
        }

        if (node.getLogdir() == null || node.getLogdir().length() == 0) {
            if (cluster.getLogdir() == null || cluster.getLogdir().length() == 0) {
                valid = false;
                LOGGER.fatal("log_dir not defined at cluster/node level for node: " + node.getId() + ERROR);
            }
        }

        if (node.getStore() == null || cluster.getStore().length() == 0) {
            if (cluster.getMasterNode().getId().equals(node.getId())
                    && (cluster.getStore() == null || cluster.getStore().length() == 0)) {
                valid = false;
                LOGGER.fatal("store not defined at cluster/node level for node: " + node.getId() + ERROR);
            }
        }

        if (node.getRam() == null || node.getRam().length() == 0) {
            if (cluster.getRam() == null || cluster.getRam().length() == 0) {
                valid = false;
                LOGGER.fatal("ram not defined at cluster/node level for node: " + node.getId() + ERROR);
            }
        }
        return valid;
    }

    private boolean checkTemporaryPath(String logdir) {
        return logdir.startsWith("/tmp/");

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

        if (zk.getServers().getServer().isEmpty()) {
            valid = false;
            LOGGER.fatal("Zookeeper servers not configured" + ERROR);
        }

        boolean validEnsemble = true;
        for (String server : zk.getServers().getServer()) {
            validEnsemble = validEnsemble && checkNodeReachability(server);
        }

        return valid;
    }

    private boolean checkNodeReachability(String server) {
        boolean reachable = true;
        try {
            InetAddress address = InetAddress.getByName(server);
            if (!address.isReachable(1000)) {
                LOGGER.fatal("\n" + "Server: " + server + " unreachable" + ERROR);
                reachable = false;
            }
        } catch (Exception e) {
            reachable = false;
            LOGGER.fatal("\n" + "Server: " + server + " Invalid address" + ERROR);
        }
        return reachable;
    }

}

class ValidateConfig extends AbstractCommandConfig {

    @Option(name = "-c", required = false, usage = "Path to the cluster configuration xml")
    public String cluster;

}
