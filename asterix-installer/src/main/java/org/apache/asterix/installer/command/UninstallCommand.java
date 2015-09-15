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

import org.kohsuke.args4j.Option;

import org.apache.asterix.event.management.AsterixEventServiceClient;
import org.apache.asterix.event.model.AsterixInstance;
import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.event.schema.pattern.Patterns;
import org.apache.asterix.event.service.AsterixEventService;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.event.service.ILookupService;
import org.apache.asterix.event.service.ServiceProvider;
import org.apache.asterix.event.util.PatternCreator;
import org.apache.asterix.installer.driver.InstallerDriver;

public class UninstallCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        UninstallConfig uninstallConfig = ((UninstallConfig) config);
        String instanceName = uninstallConfig.name;
        AsterixEventServiceUtil.validateAsterixInstanceExists(instanceName, State.INACTIVE);
        ILookupService lookupService = ServiceProvider.INSTANCE.getLookupService();
        AsterixInstance instance = lookupService.getAsterixInstance(instanceName);
        PatternCreator pc = PatternCreator.INSTANCE;
        Patterns patterns = pc.getLibraryUninstallPattern(instance, uninstallConfig.dataverseName,
                uninstallConfig.libraryName);
        AsterixEventServiceClient client = AsterixEventService.getAsterixEventServiceClient(instance.getCluster());
        client.submit(patterns);
        LOGGER.info("Uninstalled library " + uninstallConfig.libraryName);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new UninstallConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "Uninstalls a library from an asterix instance." + "\n" + "Arguments/Options\n"
                + "-n  Name of Asterix Instance\n"
                + "-d  Name of the dataverse under which the library will be installed\n" + "-l  Name of the library\n"
                + "-l  Name of the library";
    }

}

class UninstallConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-d", required = true, usage = "Name of the dataverse under which the library will be installed")
    public String dataverseName;

    @Option(name = "-l", required = true, usage = "Name of the library")
    public String libraryName;

}
