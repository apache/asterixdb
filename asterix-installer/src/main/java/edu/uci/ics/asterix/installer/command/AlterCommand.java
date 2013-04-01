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

import java.util.Date;
import java.util.Properties;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.service.ILookupService;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class AlterCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig();
        String instanceName = ((AlterConfig) config).name;
        InstallerUtil.validateAsterixInstanceExists(instanceName, State.INACTIVE);
        ILookupService lookupService = ServiceProvider.INSTANCE.getLookupService();
        AsterixInstance instance = lookupService.getAsterixInstance(instanceName);

        Properties asterixConfProp = InstallerUtil.getAsterixConfiguration(((AlterConfig) config).confPath);
        instance.setConfiguration(asterixConfProp);
        instance.setModifiedTimestamp(new Date());
        lookupService.updateAsterixInstance(instance);
        LOGGER.info("Configuration for Asterix instance: " + instanceName + " has been altered");
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new AlterConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nAlter the instance's configuration settings."
                + "\nPrior to running this command, the instance is required to be INACTIVE state."
                + "\n\nAvailable arguments/options" + "\n-n name of the ASTERIX instance"
                + "\n-conf path to the ASTERIX configuration file.";
    }

}

class AlterConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-conf", required = true, usage = "Path to instance configuration")
    public String confPath;

}
