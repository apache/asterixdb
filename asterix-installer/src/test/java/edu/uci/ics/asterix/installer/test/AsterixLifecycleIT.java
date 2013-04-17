package edu.uci.ics.asterix.installer.test;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.installer.command.CommandHandler;
import edu.uci.ics.asterix.installer.error.VerificationUtil;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.model.AsterixRuntimeState;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class AsterixLifecycleIT {

    public static final String ASTERIX_INSTANCE_NAME = "asterix";

    private static final int NUM_NC = 1;
    private static final CommandHandler cmdHandler = new CommandHandler();

    @BeforeClass
    public static void setUp() throws Exception {
        AsterixInstallerIntegrationUtil.init();
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

    @Test
    public void testStopActiveInstance() throws Exception {
        try {
            AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.ACTIVE);
            String command = "stop -n " + ASTERIX_INSTANCE_NAME;
            cmdHandler.processCommand(command.split(" "));
            AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(
                    ASTERIX_INSTANCE_NAME);
            AsterixRuntimeState state = VerificationUtil.getAsterixRuntimeState(instance);
            assert (state.getFailedNCs().size() == NUM_NC && !state.isCcRunning());
            System.out.println("Test stop active instance PASSED");
        } catch (Exception e) {
            throw new Exception("Test configure installer " + "\" FAILED!", e);
        }
    }

    @Test
    public void testStartActiveInstance() throws Exception {
        try {
            AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.INACTIVE);
            String command = "start -n " + ASTERIX_INSTANCE_NAME;
            cmdHandler.processCommand(command.split(" "));
            AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(
                    ASTERIX_INSTANCE_NAME);
            AsterixRuntimeState state = VerificationUtil.getAsterixRuntimeState(instance);
            assert (state.getFailedNCs().size() == 0 && state.isCcRunning());
            System.out.println("Test start active instance PASSED");
        } catch (Exception e) {
            throw new Exception("Test configure installer " + "\" FAILED!", e);
        }
    }

    @Test
    public void testDeleteActiveInstance() throws Exception {
        try {
            AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.INACTIVE);
            String command = "delete -n " + ASTERIX_INSTANCE_NAME;
            cmdHandler.processCommand(command.split(" "));
            AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(
                    ASTERIX_INSTANCE_NAME);
            assert (instance == null);
            System.out.println("Test delete active instance PASSED");
        } catch (Exception e) {
            throw new Exception("Test delete active instance " + "\" FAILED!", e);
        } finally {
            // recreate instance
            AsterixInstallerIntegrationUtil.createInstance();
        }
    }

}
