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

import java.util.Date;

import org.kohsuke.args4j.Option;

import org.apache.asterix.common.configuration.AsterixConfiguration;
import org.apache.asterix.event.model.AsterixInstance;
import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.event.service.ILookupService;
import org.apache.asterix.event.service.ServiceProvider;
import org.apache.asterix.installer.driver.InstallerDriver;
import org.apache.asterix.installer.driver.InstallerUtil;

public class AlterCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String instanceName = ((AlterConfig) config).name;
        AsterixEventServiceUtil.validateAsterixInstanceExists(instanceName, State.INACTIVE);
        ILookupService lookupService = ServiceProvider.INSTANCE.getLookupService();
        AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(instanceName);
        AsterixEventServiceUtil.createClusterProperties(instance.getCluster(), instance.getAsterixConfiguration());
        AsterixConfiguration asterixConfiguration = InstallerUtil
                .getAsterixConfiguration(((AlterConfig) config).confPath);
        instance.setAsterixConfiguration(asterixConfiguration);
        instance.setModifiedTimestamp(new Date());
        lookupService.updateAsterixInstance(instance);
        LOGGER.info("Altered configuration settings for Asterix instance: " + instanceName);

    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new AlterConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nAlter the instance's configuration settings."
                + "\nPrior to running this command, the instance is required to be INACTIVE state."
                + "\nChanged configuration settings will be reflected when the instance is started."
                + "\n\nAvailable arguments/options" + "\n-n name of the ASTERIX instance.";
    }

}

class AlterConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-a", required = true, usage = "Path to asterix instance configuration")
    public String confPath;

}
