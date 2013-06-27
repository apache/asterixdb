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
import java.io.FileOutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import edu.uci.ics.asterix.event.management.EventUtil;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.WorkingDir;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;

public class ConfigureCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        String localClusterPath = InstallerDriver.getManagixHome() + File.separator + "clusters" + File.separator
                + "local" + File.separator + "local.xml";

        Cluster cluster = EventUtil.getCluster(localClusterPath);
        String workingDir = InstallerDriver.getManagixHome() + File.separator + "clusters" + File.separator + "local"
                + File.separator + "working_dir";
        cluster.setWorkingDir(new WorkingDir(workingDir, true));
        cluster.setIodevices(workingDir);
        cluster.setStore("storage");
        cluster.setLogDir(workingDir + File.separator + "logs");
        cluster.setTxnLogDir(workingDir + File.separator + "txnLogs");
        cluster.setJavaHome(System.getProperty("java.home"));

        JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(cluster, new FileOutputStream(localClusterPath));

        String installerConfPath = InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_CONF_XML;
        ctx = JAXBContext.newInstance(Configuration.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        Configuration configuration = (Configuration) unmarshaller.unmarshal(new File(installerConfPath));

        configuration.getBackup().setBackupDir(workingDir + File.separator + "backup");
        configuration.getZookeeper().setHomeDir(
                InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_INTERNAL_DIR
                        + File.separator + "zookeeper_home");
        configuration.getZookeeper().getServers().setJavaHome(System.getProperty("java.home"));

        marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(configuration, new FileOutputStream(installerConfPath));

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
