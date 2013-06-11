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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.management.EventrixClient;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.pattern.Pattern;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class StopCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((StopConfig) config).name;
        AsterixInstance asterixInstance = InstallerUtil.validateAsterixInstanceExists(asterixInstanceName,
                State.ACTIVE, State.UNUSABLE);
        PatternCreator pc = new PatternCreator();
        EventrixClient client = InstallerUtil.getEventrixClient(asterixInstance.getCluster());

        List<Pattern> ncKillPatterns = new ArrayList<Pattern>();
        for (Node node : asterixInstance.getCluster().getNode()) {
            ncKillPatterns.add(pc.createNCStopPattern(node.getId(), asterixInstanceName + "_" + node.getId()));
        }

        List<Pattern> ccKillPatterns = new ArrayList<Pattern>();
        ccKillPatterns.add(pc.createCCStopPattern(asterixInstance.getCluster().getMasterNode().getId()));

        try {
            client.submit(new Patterns(ncKillPatterns));
            client.submit(new Patterns(ccKillPatterns));
        } catch (Exception e) {
            // processes are already dead
            LOGGER.debug("Attempt to kill non-existing processess");
        }

        asterixInstance.setState(State.INACTIVE);
        asterixInstance.setStateChangeTimestamp(new Date());
        ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(asterixInstance);
        LOGGER.info("Stopped Asterix instance: " + asterixInstanceName);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new StopConfig();
    }

    public String getAsterixInstanceName() {
        return ((StopConfig) config).name;
    }

    @Override
    protected String getUsageDescription() {
        return "\nShuts an ASTERIX instance that is in ACTIVE/UNUSABLE state."
                + "\nAfter executing the stop command, the ASTERIX instance transits"
                + "\nto the INACTIVE state, indicating that it is no longer available"
                + "\nfor executing statements/queries." + "\n\nAvailable arguments/options"
                + "\n-n name of the ASTERIX instance.";

    }

}

class StopConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

}
