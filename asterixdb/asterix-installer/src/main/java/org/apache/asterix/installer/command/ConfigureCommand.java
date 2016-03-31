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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.event.management.EventUtil;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.event.schema.cluster.WorkingDir;
import org.apache.asterix.installer.driver.InstallerDriver;
import org.apache.asterix.installer.schema.conf.Configuration;

public class ConfigureCommand extends AbstractCommand {

    private static final String WORK_DIR = "/tmp/asterix";

    @Override
    protected void execCommand() throws Exception {
        configureCluster("local", "local.xml");
        configureCluster("demo", "demo.xml");

        String installerConfPath = InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_CONF_XML;
        JAXBContext ctx = JAXBContext.newInstance(Configuration.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        Configuration configuration = (Configuration) unmarshaller.unmarshal(new File(installerConfPath));

        configuration.setConfigured(true);
        configuration.getBackup().setBackupDir(InstallerDriver.getManagixHome() + File.separator + "backup");
        configuration.getZookeeper().setHomeDir(
                InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_INTERNAL_DIR
                        + File.separator + "zookeeper_home");
        configuration.getZookeeper().getServers().setJavaHome(System.getProperty("java.home"));

        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(configuration, new FileOutputStream(installerConfPath));
    }

    private void configureCluster(String dir, String file) throws JAXBException, PropertyException,
            FileNotFoundException {
        String clusterDir = InstallerDriver.getManagixHome() + File.separator + "clusters" + File.separator + dir;
        String localClusterPath = clusterDir + File.separator + file;

        Cluster cluster = EventUtil.getCluster(localClusterPath);
        String workingDir = clusterDir + File.separator + "working_dir";
        cluster.setWorkingDir(new WorkingDir(workingDir, true));
        cluster.setIodevices(configureIoDevices(cluster.getIodevices(), workingDir));
        cluster.setLogDir(configureDirectory(cluster.getLogDir(), workingDir));
        cluster.setTxnLogDir(configureDirectory(cluster.getTxnLogDir(), workingDir));
        cluster.setJavaHome(System.getProperty("java.home"));

        for (Node node : cluster.getNode()) {
            node.setIodevices(configureIoDevices(node.getIodevices(), workingDir));
            node.setLogDir(configureDirectory(node.getLogDir(), workingDir));
            node.setTxnLogDir(configureDirectory(node.getTxnLogDir(), workingDir));
        }

        JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(cluster, new FileOutputStream(localClusterPath));
    }

    private String configureIoDevices(String ioDevices, String workingDir) {
        if (ioDevices == null) {
            return null;
        }
        final String separator = ",";
        StringBuilder sb = new StringBuilder();
        String[] ioDevs = ioDevices.split(separator);
        for (int i = 0; i < ioDevs.length; ++i) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(configureDirectory(ioDevs[i], workingDir));
        }
        return sb.toString();
    }

    private String configureDirectory(String dir, String workingDir) {
        return dir == null ? null : dir.replace(WORK_DIR, workingDir);
    }

    @Override
    protected String getUsageDescription() {
        return "\nAuto-generates the ASTERIX installer configruation settings and ASTERIX cluster "
                + "\nconfiguration settings for a single node setup.";
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new ConfigureConfig();
    }

}

class ConfigureConfig extends CommandConfig {

}
