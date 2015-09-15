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

import java.io.File;

import org.kohsuke.args4j.Option;

import org.apache.asterix.event.error.VerificationUtil;
import org.apache.asterix.event.management.AsterixEventServiceClient;
import org.apache.asterix.event.model.AsterixInstance;
import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.event.model.AsterixRuntimeState;
import org.apache.asterix.event.schema.pattern.Patterns;
import org.apache.asterix.event.service.AsterixEventService;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.event.service.ServiceProvider;
import org.apache.asterix.event.util.PatternCreator;
import org.apache.asterix.installer.driver.InstallerDriver;

public class StartCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((StartConfig) config).name;
        AsterixInstance instance = AsterixEventServiceUtil.validateAsterixInstanceExists(asterixInstanceName,
                State.INACTIVE);
        AsterixEventServiceUtil.createAsterixZip(instance);
        AsterixEventServiceClient client = AsterixEventService.getAsterixEventServiceClient(instance.getCluster());
        Patterns asterixBinaryTransferPattern = PatternCreator.INSTANCE.getAsterixBinaryTransferPattern(
                asterixInstanceName, instance.getCluster());
        client.submit(asterixBinaryTransferPattern);
        AsterixEventServiceUtil.createClusterProperties(instance.getCluster(), instance.getAsterixConfiguration());
        Patterns patterns = PatternCreator.INSTANCE.getStartAsterixPattern(asterixInstanceName, instance.getCluster(), false);
        client.submit(patterns);
        AsterixEventServiceUtil.deleteDirectory(InstallerDriver.getManagixHome() + File.separator
                + InstallerDriver.ASTERIX_DIR + File.separator + asterixInstanceName);
        AsterixRuntimeState runtimeState = VerificationUtil.getAsterixRuntimeState(instance);
        VerificationUtil.updateInstanceWithRuntimeDescription(instance, runtimeState, true);
        LOGGER.info(instance.getDescription(false));
        ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(instance);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new StartConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nStarts an ASTERIX instance that is in INACTIVE state."
                + "\nAfter executing the start command, the ASTERIX instance transits to the ACTIVE state,"
                + "\nindicating that it is now available for executing statements/queries."
                + "\n\nAvailable arguments/options" + "\n-n name of the ASTERIX instance. ";
    }
}

class StartConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

}
