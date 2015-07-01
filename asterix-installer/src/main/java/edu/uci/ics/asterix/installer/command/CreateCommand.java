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

import java.io.File;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.common.configuration.AsterixConfiguration;
import edu.uci.ics.asterix.event.error.VerificationUtil;
import edu.uci.ics.asterix.event.management.AsterixEventServiceClient;
import edu.uci.ics.asterix.event.management.EventUtil;
import edu.uci.ics.asterix.event.model.AsterixInstance;
import edu.uci.ics.asterix.event.model.AsterixRuntimeState;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.event.service.AsterixEventService;
import edu.uci.ics.asterix.event.service.AsterixEventServiceUtil;
import edu.uci.ics.asterix.event.service.ServiceProvider;
import edu.uci.ics.asterix.event.util.PatternCreator;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;

public class CreateCommand extends AbstractCommand {

    private String asterixInstanceName;
    private Cluster cluster;
    private AsterixConfiguration asterixConfiguration;

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        ValidateCommand validateCommand = new ValidateCommand();
        boolean valid = validateCommand.validateCluster(((CreateConfig) config).clusterPath);
        if (!valid) {
            throw new Exception("Cannot create an Asterix instance.");
        }
        asterixInstanceName = ((CreateConfig) config).name;
        AsterixEventServiceUtil.validateAsterixInstanceNotExists(asterixInstanceName);
        CreateConfig createConfig = (CreateConfig) config;
        cluster = EventUtil.getCluster(createConfig.clusterPath);
        cluster.setInstanceName(asterixInstanceName);
        asterixConfiguration = InstallerUtil.getAsterixConfiguration(createConfig.asterixConfPath);
        AsterixInstance asterixInstance = AsterixEventServiceUtil.createAsterixInstance(asterixInstanceName, cluster,
                asterixConfiguration);
        AsterixEventServiceUtil.evaluateConflictWithOtherInstances(asterixInstance);
        AsterixEventServiceUtil.createAsterixZip(asterixInstance);
        AsterixEventServiceUtil.createClusterProperties(cluster, asterixConfiguration);
        AsterixEventServiceClient eventrixClient = AsterixEventService.getAsterixEventServiceClient(cluster, true,
                false);

        Patterns asterixBinarytrasnferPattern = PatternCreator.INSTANCE.getAsterixBinaryTransferPattern(
                asterixInstanceName, cluster);
        eventrixClient.submit(asterixBinarytrasnferPattern);

        Patterns patterns = PatternCreator.INSTANCE.getStartAsterixPattern(asterixInstanceName, cluster, true);
        eventrixClient.submit(patterns);

        AsterixRuntimeState runtimeState = VerificationUtil.getAsterixRuntimeState(asterixInstance);
        VerificationUtil.updateInstanceWithRuntimeDescription(asterixInstance, runtimeState, true);
        ServiceProvider.INSTANCE.getLookupService().writeAsterixInstance(asterixInstance);
        AsterixEventServiceUtil.deleteDirectory(InstallerDriver.getManagixHome() + File.separator
                + InstallerDriver.ASTERIX_DIR + File.separator + asterixInstanceName);
        LOGGER.info(asterixInstance.getDescription(false));
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new CreateConfig();
    }

    public Cluster getCluster() {
        return cluster;
    }

    public String getAsterixInstanceName() {
        return asterixInstanceName;
    }

    @Override
    protected String getUsageDescription() {
        return "\nCreates an ASTERIX instance with a specified name."
                + "\n\nPost creation, the instance is in ACTIVE state, indicating its "
                + "\navailability for executing statements/queries." + "\n\nUsage arguments/options:"
                + "\n-n Name of the ASTERIX instance." + "\n-c Path to the cluster configuration file"
                + "\n[-a] Path to asterix configuration file" + "\n [..] indicates optional flag";
    }

}

class CreateConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-c", required = true, usage = "Path to cluster configuration")
    public String clusterPath;

    @Option(name = "-a", required = false, usage = "Path to asterix configuration")
    public String asterixConfPath;

}
