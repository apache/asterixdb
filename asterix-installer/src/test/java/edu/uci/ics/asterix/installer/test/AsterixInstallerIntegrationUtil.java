package edu.uci.ics.asterix.installer.test;

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

import edu.uci.ics.asterix.installer.command.CommandHandler;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.error.VerificationUtil;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.model.AsterixRuntimeState;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class AsterixInstallerIntegrationUtil {

    private static String managixHome;
    private static String clusterConfigurationPath;
    private static final CommandHandler cmdHandler = new CommandHandler();
    public static final String ASTERIX_INSTANCE_NAME = "asterix";

    public static void deinit() throws Exception {
        deleteInstance();
        stopZookeeper();
    }

    public static void init() throws Exception {
        File asterixProjectDir = new File(System.getProperty("user.dir"));
        asterixProjectDir = new File("/Users/ramang/research/work/asterix/git-branches/asterixdb/asterix-installer");
        File installerTargetDir = new File(asterixProjectDir, "target");
        System.out.println("asterix project dir" + asterixProjectDir.getAbsolutePath());
        System.out.println("installer target dir" + installerTargetDir.getAbsolutePath());
        String managixHomeDirName = installerTargetDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory() && name.startsWith("asterix-installer")
                        && name.endsWith("binary-assembly");
            }

        })[0];
        managixHome = new File(installerTargetDir, managixHomeDirName).getAbsolutePath();
        System.out.println("Setting managix home to :" + managixHome);
        System.setProperty("log4j.configuration", managixHome + File.separator + "conf" + File.separator
                + "log4j.properties");

        managixHome = AsterixInstallerIntegrationUtil.getManagixHome();
        clusterConfigurationPath = managixHome + File.separator + "clusters" + File.separator + "local"
                + File.separator + "local.xml";

        InstallerDriver.setManagixHome(managixHome);

        String command = "configure";
        cmdHandler.processCommand(command.split(" "));
        command = "validate -c " + clusterConfigurationPath;
        cmdHandler.processCommand(command.split(" "));

        startZookeeper();
        InstallerDriver.initConfig();
        createInstance();
    }

    private static void startZookeeper() throws IOException, JAXBException, InterruptedException {
        initZookeeperTestConfiguration();
        String script = managixHome + File.separator + "bin" + File.separator + "managix";

        // shutdown zookeeper if running
        ProcessBuilder pb = new ProcessBuilder(script, "shutdown");
        Map<String, String> env = pb.environment();
        env.put("MANAGIX_HOME", managixHome);
        pb.start();
        Thread.sleep(2000);

        // start zookeeper 
        ProcessBuilder pb2 = new ProcessBuilder(script, "describe");
        Map<String, String> env2 = pb2.environment();
        env2.put("MANAGIX_HOME", managixHome);
        pb2.start();

        Thread.sleep(2000);
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

    private static void initZookeeperTestConfiguration() throws JAXBException, FileNotFoundException {
        String installerConfPath = InstallerDriver.getManagixHome() + File.separator + InstallerDriver.MANAGIX_CONF_XML;
        JAXBContext ctx = JAXBContext.newInstance(Configuration.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        Configuration configuration = (Configuration) unmarshaller.unmarshal(new File(installerConfPath));
        configuration.getZookeeper().setClientPort(new BigInteger("3945"));
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
                command = "start -n" + ASTERIX_INSTANCE_NAME;
                break;
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
        return managixHome;
    }

}
