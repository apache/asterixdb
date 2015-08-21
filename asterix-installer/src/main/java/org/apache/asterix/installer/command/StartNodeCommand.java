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
import java.util.List;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.error.VerificationUtil;
import edu.uci.ics.asterix.event.management.AsterixEventServiceClient;
import edu.uci.ics.asterix.event.model.AsterixInstance;
import edu.uci.ics.asterix.event.model.AsterixInstance.State;
import edu.uci.ics.asterix.event.model.AsterixRuntimeState;
import edu.uci.ics.asterix.event.model.ProcessInfo;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.event.schema.pattern.Pattern;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.event.service.AsterixEventService;
import edu.uci.ics.asterix.event.service.AsterixEventServiceUtil;
import edu.uci.ics.asterix.event.service.ServiceProvider;
import edu.uci.ics.asterix.event.util.PatternCreator;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.error.InstallerException;

public class StartNodeCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((StartNodeConfig) config).name;
        AsterixInstance instance = AsterixEventServiceUtil.validateAsterixInstanceExists(asterixInstanceName,
                State.INACTIVE, State.ACTIVE, State.UNUSABLE);

        Cluster cluster = instance.getCluster();
        List<Pattern> pl = new ArrayList<Pattern>();
        AsterixRuntimeState runtimeState = VerificationUtil.getAsterixRuntimeState(instance);
        String[] nodesToBeAdded = ((StartNodeConfig) config).nodes.split(",");
        List<String> aliveNodes = new ArrayList<String>();
        for (ProcessInfo p : runtimeState.getProcesses()) {
            aliveNodes.add(p.getNodeId());
        }
        List<Node> clusterNodes = cluster.getNode();
        for (String n : nodesToBeAdded) {
            if (aliveNodes.contains(n)) {
                throw new InstallerException("Node: " + n + " is already alive");
            }
            for (Node node : clusterNodes) {
                if (n.equals(node.getId())) {
                    String iodevices = node.getIodevices() == null ? cluster.getIodevices() : node.getIodevices();
                    Pattern createNC = PatternCreator.INSTANCE.createNCStartPattern(cluster.getMasterNode()
                            .getClusterIp(), node.getId(), asterixInstanceName + "_" + node.getId(), iodevices, false);
                    pl.add(createNC);
                    break;
                }
            }
        }
        Patterns patterns = new Patterns(pl);
        AsterixEventServiceClient client = AsterixEventService.getAsterixEventServiceClient(cluster);
        client.submit(patterns);
        runtimeState = VerificationUtil.getAsterixRuntimeState(instance);
        VerificationUtil.updateInstanceWithRuntimeDescription(instance, runtimeState, true);
        LOGGER.info(instance.getDescription(false));
        ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(instance);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new StartNodeConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nStarts a set of nodes for an ASTERIX instance." + "\n\nAvailable arguments/options"
                + "\n-n name of the ASTERIX instance. " + "\n-nodes"
                + "Comma separated list of nodes that need to be started";
    }
}

class StartNodeConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-nodes", required = true, usage = "Comma separated list of nodes that need to be started")
    public String nodes;

}
