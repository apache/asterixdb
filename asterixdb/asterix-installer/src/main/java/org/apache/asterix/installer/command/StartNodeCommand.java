/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.installer.command;

import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Option;

import org.apache.asterix.event.error.VerificationUtil;
import org.apache.asterix.event.management.AsterixEventServiceClient;
import org.apache.asterix.event.model.AsterixInstance;
import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.event.model.AsterixRuntimeState;
import org.apache.asterix.event.model.ProcessInfo;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.event.schema.pattern.Pattern;
import org.apache.asterix.event.schema.pattern.Patterns;
import org.apache.asterix.event.service.AsterixEventService;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.event.service.ServiceProvider;
import org.apache.asterix.event.util.PatternCreator;
import org.apache.asterix.installer.driver.InstallerDriver;
import org.apache.asterix.installer.error.InstallerException;

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
