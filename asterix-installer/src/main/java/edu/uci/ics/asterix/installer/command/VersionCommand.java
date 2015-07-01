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

import edu.uci.ics.asterix.event.service.AsterixEventService;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;

public class VersionCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(false);
        String asterixZipName = AsterixEventService.getAsterixZip().substring(
                AsterixEventService.getAsterixZip().lastIndexOf(File.separator) + 1);
        String asterixVersion = asterixZipName.substring("asterix-server-".length(),
                asterixZipName.indexOf("-binary-assembly"));
        LOGGER.info("Asterix/Managix version " + asterixVersion);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new VersionConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "Provides version of Managix/Asterix";
    }

}

class VersionConfig extends CommandConfig {

}
