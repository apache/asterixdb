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

import java.io.File;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.management.EventrixClient;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.error.InstallerException;
import edu.uci.ics.asterix.installer.error.VerificationUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixRuntimeState;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class LogCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig();
        String asterixInstanceName = ((LogConfig) config).name;
        AsterixInstance instance = InstallerUtil.validateAsterixInstanceExists(asterixInstanceName, State.INACTIVE);
        PatternCreator pc = new PatternCreator();
        EventrixClient client = InstallerUtil.getEventrixClient(instance.getCluster());
        String outputDir = ((LogConfig) config).outputDir == null ? instance.getCluster().getWorkingDir().getDir() 
                : ((LogConfig) config).outputDir;
        File f = new File(outputDir);
        if (!f.exists()) {
            boolean success = f.mkdirs();
            if (!success) {
                throw new InstallerException("Unable to create output directory:" + outputDir);
            }
        }
        Patterns transferLogPattern = pc.getTransferLogPattern(asterixInstanceName, instance.getCluster(), outputDir);
        client.submit(transferLogPattern);
        LOGGER.info("Log tar ball located at " + outputDir);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new LogConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nCreates a tar ball containing log files corresponding to each worker node (NC) and the master (CC)  for an ASTERIX instance"
                + "\n\nAvailable arguments/options"
                + "\n-n name of the ASTERIX instance. \n-d destination directory for producing the tar ball (defaults to) "
                + InstallerDriver.getManagixHome();
    }
}

class LogConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-d", required = false, usage = "Destination directory for producing log tar ball")
    public String outputDir;

}
