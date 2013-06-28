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

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class DeleteCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((DeleteConfig) config).name;
        AsterixInstance instance = InstallerUtil.validateAsterixInstanceExists(asterixInstanceName, State.INACTIVE);
        PatternCreator pc = new PatternCreator();
        Patterns patterns = pc.createDeleteInstancePattern(instance);
        InstallerUtil.getEventrixClient(instance.getCluster()).submit(patterns);

        patterns = pc.createRemoveAsterixWorkingDirPattern(instance);
        InstallerUtil.getEventrixClient(instance.getCluster()).submit(patterns);
        ServiceProvider.INSTANCE.getLookupService().removeAsterixInstance(asterixInstanceName);
        LOGGER.info("Deleted Asterix instance: " + asterixInstanceName);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new DeleteConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nPermanently deletes an ASTERIX instance." + "\n" + "The instance must be in the INACTIVE state."
                + "\n\nAvailable arguments/options" + "\n-n name of the ASTERIX instance.";
    }

}

class DeleteConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

}
