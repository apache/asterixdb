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
import java.util.Date;
import java.util.List;

import org.kohsuke.args4j.Option;

import org.apache.asterix.event.error.VerificationUtil;
import org.apache.asterix.event.management.AsterixEventServiceClient;
import org.apache.asterix.event.model.AsterixInstance;
import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.event.model.AsterixRuntimeState;
import org.apache.asterix.event.model.ProcessInfo;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.event.schema.pattern.Pattern;
import org.apache.asterix.event.schema.pattern.Patterns;
import org.apache.asterix.event.service.AsterixEventService;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.event.service.ServiceProvider;
import org.apache.asterix.event.util.PatternCreator;
import org.apache.asterix.installer.driver.InstallerDriver;
import org.apache.asterix.installer.error.InstallerException;

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
