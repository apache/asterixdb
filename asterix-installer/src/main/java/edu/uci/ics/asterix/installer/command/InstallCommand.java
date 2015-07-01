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

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.model.AsterixInstance;
import edu.uci.ics.asterix.event.model.AsterixInstance.State;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.event.service.AsterixEventService;
import edu.uci.ics.asterix.event.service.AsterixEventServiceUtil;
import edu.uci.ics.asterix.event.util.PatternCreator;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;

public class InstallCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        InstallConfig installConfig = ((InstallConfig) config);
        String instanceName = installConfig.name;
        AsterixInstance instance = AsterixEventServiceUtil.validateAsterixInstanceExists(instanceName, State.INACTIVE);
        PatternCreator pc = PatternCreator.INSTANCE;
        Patterns patterns = pc.getLibraryInstallPattern(instance, installConfig.dataverseName,
                installConfig.libraryName, installConfig.libraryPath);
        AsterixEventService.getAsterixEventServiceClient(instance.getCluster()).submit(patterns);
        LOGGER.info("Installed library " + installConfig.libraryName);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new InstallConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "Installs a library to an asterix instance." + "\n" + "Arguments/Options\n"
                + "-n  Name of Asterix Instance\n"
                + "-d  Name of the dataverse under which the library will be installed\n" + "-l  Name of the library\n"
                + "-p  Path to library zip bundle";

    }

}

class InstallConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-d", required = true, usage = "Name of the dataverse under which the library will be installed")
    public String dataverseName;

    @Option(name = "-l", required = true, usage = "Name of the library")
    public String libraryName;

    @Option(name = "-p", required = true, usage = "Path to library zip bundle")
    public String libraryPath;

}
