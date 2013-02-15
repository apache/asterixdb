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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.driver.EventDriver;
import edu.uci.ics.asterix.event.management.EventrixClient;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.pattern.Pattern;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.ManagixUtil;
import edu.uci.ics.asterix.installer.error.OutputHandler;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class StopCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        String asterixInstanceName = ((StopConfig) config).name;
        AsterixInstance asterixInstance = ManagixUtil.validateAsterixInstanceExists(asterixInstanceName, State.ACTIVE,
                State.UNUSABLE);
        PatternCreator pc = new PatternCreator();
        List<Pattern> patternsToExecute = new ArrayList<Pattern>();
        patternsToExecute.add(pc.createCCStopPattern(asterixInstance.getCluster().getMasterNode().getId()));

        for (Node node : asterixInstance.getCluster().getNode()) {
            patternsToExecute.add(pc.createNCStopPattern(node.getId(), asterixInstanceName + "_" + node.getId()));
        }
        EventrixClient client = EventDriver.getClient(asterixInstance.getCluster(), false, OutputHandler.INSTANCE);
        try {
            client.submit(new Patterns(patternsToExecute));
        } catch (Exception e) {
            // processes are already dead
        }
        asterixInstance.setState(State.INACTIVE);
        asterixInstance.setStateChangeTimestamp(new Date());
        ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(asterixInstance);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new StopConfig();
    }

    public String getAsterixInstanceName() {
        return ((StopConfig) config).name;
    }

}

class StopConfig implements CommandConfig {

    @Option(name = "-h", required = false, usage = "Help")
    public boolean help = false;

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

}
