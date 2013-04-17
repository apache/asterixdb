package edu.uci.ics.asterix.installer.command;

import java.io.File;
import java.io.FileOutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.WorkingDir;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;

public class ConfigureCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        String localClusterPath = InstallerDriver.getManagixHome() + File.separator + "clusters" + File.separator
                + "local" + File.separator + "local.xml";

        JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        Cluster cluster = (Cluster) unmarshaller.unmarshal(new File(localClusterPath));

        String workingDir = InstallerDriver.getManagixHome() + File.separator + "clusters" + File.separator + "local"
                + File.separator + "working_dir";
        cluster.setWorkingDir(new WorkingDir(workingDir, true));
        cluster.setIodevices(workingDir);
        cluster.setStore("storage");
        cluster.setLogdir(workingDir + File.separator + "logs");
        cluster.setJavaHome(System.getProperty("java.home"));
        cluster.setJavaOpts("-Xmx1024m");

        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(cluster, new FileOutputStream(localClusterPath));

        String installerConfPath = InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_CONF_XML;
        ctx = JAXBContext.newInstance(Configuration.class);
        unmarshaller = ctx.createUnmarshaller();
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
