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

import edu.uci.ics.asterix.event.error.VerificationUtil;
import edu.uci.ics.asterix.event.management.AsterixEventServiceClient;
import edu.uci.ics.asterix.event.model.AsterixInstance;
import edu.uci.ics.asterix.event.model.AsterixInstance.State;
import edu.uci.ics.asterix.event.model.AsterixRuntimeState;
import edu.uci.ics.asterix.event.model.ProcessInfo;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.pattern.Pattern;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.event.service.AsterixEventService;
import edu.uci.ics.asterix.event.service.AsterixEventServiceUtil;
import edu.uci.ics.asterix.event.service.ServiceProvider;
import edu.uci.ics.asterix.event.util.PatternCreator;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.error.InstallerException;

public class StopNodeCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((StopNodeConfig) config).name;
        AsterixInstance asterixInstance = AsterixEventServiceUtil.validateAsterixInstanceExists(asterixInstanceName,
                State.ACTIVE, State.UNUSABLE);

        AsterixEventServiceClient client = AsterixEventService.getAsterixEventServiceClient(asterixInstance
                .getCluster());

        String[] nodesToStop = ((StopNodeConfig) config).nodeList.split(",");
        AsterixRuntimeState runtimeState = VerificationUtil.getAsterixRuntimeState(asterixInstance);
        List<String> aliveNodes = new ArrayList<String>();
        for (ProcessInfo p : runtimeState.getProcesses()) {
            aliveNodes.add(p.getNodeId());
        }

        List<String> validNodeIds = new ArrayList<String>();
        for (Node node : asterixInstance.getCluster().getNode()) {
            validNodeIds.add(node.getId());
        }
        List<Pattern> ncKillPatterns = new ArrayList<Pattern>();
        for (String nodeId : nodesToStop) {
            if (!nodeId.contains(nodeId)) {
                throw new InstallerException("Invalid nodeId: " + nodeId);
            }
            if (!aliveNodes.contains(nodeId)) {
                throw new InstallerException("Node: " + nodeId + " is not alive");
            }
            ncKillPatterns.add(PatternCreator.INSTANCE.createNCStopPattern(nodeId, asterixInstanceName + "_" + nodeId));
        }

        try {
            client.submit(new Patterns(ncKillPatterns));
        } catch (Exception e) {
            // processes are already dead
            LOGGER.debug("Attempt to kill non-existing processess");
        }

        asterixInstance.setStateChangeTimestamp(new Date());
        ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(asterixInstance);
        LOGGER.info("Stopped nodes " + ((StopNodeConfig) config).nodeList + " serving Asterix instance: "
                + asterixInstanceName);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new StopNodeConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nStops a specified set of ASTERIX nodes." + "\n\nAvailable arguments/options"
                + "\n-n name of the ASTERIX instance. "
                + "\n-nodes Comma separated list of nodes that need to be stopped. ";

    }
}

class StopNodeConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-nodes", required = true, usage = "Comma separated list of nodes that need to be stopped")
    public String nodeList;

}
