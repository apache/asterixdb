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

import org.apache.asterix.event.management.AsterixEventServiceClient;
import org.apache.asterix.event.model.AsterixInstance;
import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.event.schema.pattern.Pattern;
import org.apache.asterix.event.schema.pattern.Patterns;
import org.apache.asterix.event.service.AsterixEventService;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.event.service.ServiceProvider;
import org.apache.asterix.event.util.PatternCreator;
import org.apache.asterix.installer.driver.InstallerDriver;

public class StopCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((StopConfig) config).name;
        AsterixInstance asterixInstance = AsterixEventServiceUtil.validateAsterixInstanceExists(asterixInstanceName,
                State.ACTIVE, State.UNUSABLE);
        AsterixEventServiceClient client = AsterixEventService.getAsterixEventServiceClient(asterixInstance
                .getCluster());

        List<Pattern> ncKillPatterns = new ArrayList<Pattern>();
        for (Node node : asterixInstance.getCluster().getNode()) {
            ncKillPatterns.add(PatternCreator.INSTANCE.createNCStopPattern(node.getId(), asterixInstanceName + "_"
                    + node.getId()));
        }

        List<Pattern> ccKillPatterns = new ArrayList<Pattern>();
        ccKillPatterns.add(PatternCreator.INSTANCE.createCCStopPattern(asterixInstance.getCluster().getMasterNode()
                .getId()));

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
