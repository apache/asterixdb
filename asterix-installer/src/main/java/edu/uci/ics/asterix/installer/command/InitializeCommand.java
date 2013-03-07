package edu.uci.ics.asterix.installer.command;

import java.io.File;
import java.io.FileOutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.WorkingDir;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;

public class InitializeCommand extends AbstractCommand {

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
        cluster.setStore(workingDir + File.separator + "storage");
        cluster.setLogdir(workingDir + File.separator + "logs");
        cluster.setJavaHome(System.getenv("JAVA_HOME"));

        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(cluster, new FileOutputStream(localClusterPath));

        String installerConfPath = InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_CONF_XML;
        ctx = JAXBContext.newInstance(Configuration.class);
        unmarshaller = ctx.createUnmarshaller();
        Configuration configuration = (Configuration) unmarshaller.unmarshal(new File(installerConfPath));

        configuration.getZookeeper().setHomeDir(workingDir + File.separator + "zookeeper");
        marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(configuration, new FileOutputStream(installerConfPath));

    }

    @Override
    protected String getUsageDescription() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new InitializeConfig();
    }

    public static void main(String args[]) throws Exception {
        new InitializeCommand().execCommand();
    }
}

class InitializeConfig implements CommandConfig {

    @Option(name = "-h", required = false, usage = "Help")
    public boolean help = false;

}
