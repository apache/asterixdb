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

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.management.EventrixClient;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.error.VerificationUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixRuntimeState;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class StartCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((StartConfig) config).name;
        AsterixInstance instance = InstallerUtil.validateAsterixInstanceExists(asterixInstanceName, State.INACTIVE);
        InstallerUtil.createAsterixZip(instance);
        PatternCreator pc = new PatternCreator();
        EventrixClient client = InstallerUtil.getEventrixClient(instance.getCluster());
        Patterns asterixBinaryTransferPattern = pc.getAsterixBinaryTransferPattern(asterixInstanceName,
                instance.getCluster());
        client.submit(asterixBinaryTransferPattern);
        InstallerUtil.createClusterProperties(instance.getCluster(), instance.getAsterixConfiguration());
        Patterns patterns = pc.getStartAsterixPattern(asterixInstanceName, instance.getCluster());
        client.submit(patterns);
        InstallerUtil.deleteDirectory(InstallerDriver.getManagixHome() + File.separator + InstallerDriver.ASTERIX_DIR
                + File.separator + asterixInstanceName);
        AsterixRuntimeState runtimeState = VerificationUtil.getAsterixRuntimeState(instance);
        VerificationUtil.updateInstanceWithRuntimeDescription(instance, runtimeState, true);
        LOGGER.info(instance.getDescription(false));
        ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(instance);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new StartConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nStarts an ASTERIX instance that is in INACTIVE state."
                + "\nAfter executing the start command, the ASTERIX instance transits to the ACTIVE state,"
                + "\nindicating that it is now available for executing statements/queries."
                + "\n\nAvailable arguments/options" + "\n-n name of the ASTERIX instance. ";
    }
}

class StartConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

}
