package edu.uci.ics.asterix.installer.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.installer.command.CommandHandler;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.error.VerificationUtil;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.model.AsterixRuntimeState;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;
import edu.uci.ics.asterix.installer.service.ILookupService;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class InstallerIntegrationTest {

    public static final String ASTERIX_INSTANCE_NAME = "asterix";

    private static final int NUM_NC = 1;
    private static String managixHome;
    private static final CommandHandler cmdHandler = new CommandHandler();
    private static String clusterConfigurationPath;

    @BeforeClass
    public static void setUp() throws Exception {

        AsterixInstallerIntegrationUtil.init();

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

    private static void startZookeeper() throws IOException, JAXBException {
        initZookeeperTestConfiguration();
        String script = managixHome + File.separator + "bin" + File.separator + "managix";

        // shutdown zookeeper if running
        ProcessBuilder pb = new ProcessBuilder(script, "shutdown");
        Map<String, String> env = pb.environment();
        env.put("MANAGIX_HOME", managixHome);
        pb.start();

        // start zookeeper 
        ProcessBuilder pb2 = new ProcessBuilder(script, "describe");
        Map<String, String> env2 = pb2.environment();
        env2.put("MANAGIX_HOME", managixHome);
        pb2.start();
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

    @AfterClass
    public static void tearDown() throws Exception {
        AsterixInstallerIntegrationUtil.deinit();
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        return testArgs;
    }

    public InstallerIntegrationTest() {

    }

    private static void createInstance() throws Exception {

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

        AsterixRuntimeState state = VerificationUtil.getAsterixRuntimeState(instance);
        assert (state.getFailedNCs().isEmpty() && state.isCcRunning());
    }

    @Test
    public void testStopActiveInstance() throws Exception {
        try {
            transformIntoRequiredState(State.ACTIVE);
            String command = "stop -n " + ASTERIX_INSTANCE_NAME;
            cmdHandler.processCommand(command.split(" "));
            AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(
                    ASTERIX_INSTANCE_NAME);
            AsterixRuntimeState state = VerificationUtil.getAsterixRuntimeState(instance);
            assert (state.getFailedNCs().size() == NUM_NC && !state.isCcRunning());
        } catch (Exception e) {
            throw new Exception("Test configure installer " + "\" FAILED!", e);
        }
    }

    @Test
    public void testStartActiveInstance() throws Exception {
        try {
            transformIntoRequiredState(State.INACTIVE);
            String command = "start -n " + ASTERIX_INSTANCE_NAME;
            cmdHandler.processCommand(command.split(" "));
            AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(
                    ASTERIX_INSTANCE_NAME);
            AsterixRuntimeState state = VerificationUtil.getAsterixRuntimeState(instance);
            assert (state.getFailedNCs().size() == 0 && state.isCcRunning());
        } catch (Exception e) {
            throw new Exception("Test configure installer " + "\" FAILED!", e);
        }
    }

    @Test
    public void testDeleteActiveInstance() throws Exception {
        try {
            transformIntoRequiredState(State.INACTIVE);
            String command = "delete -n " + ASTERIX_INSTANCE_NAME;
            cmdHandler.processCommand(command.split(" "));
            AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(
                    ASTERIX_INSTANCE_NAME);
            assert (instance != null);
        } catch (Exception e) {
            throw new Exception("Test configure installer " + "\" FAILED!", e);
        } finally {
            // recreate instance
            createInstance();
        }
    }

    @Test
    public void testShutdownInstaller() throws Exception {
        try {
            String command = "shutdown";
            cmdHandler.processCommand(command.split(" "));
            ILookupService service = ServiceProvider.INSTANCE.getLookupService();
            assert (!service.isRunning(InstallerDriver.getConfiguration()));
        } catch (Exception e) {
            throw new Exception("Test configure installer " + "\" FAILED!", e);
        } finally {
            // recreate instance
            createInstance();
        }
    }

    private static void transformIntoRequiredState(AsterixInstance.State state) throws Exception {
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

}
