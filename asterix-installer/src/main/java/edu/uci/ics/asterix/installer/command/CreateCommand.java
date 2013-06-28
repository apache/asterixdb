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
import edu.uci.ics.asterix.event.management.EventUtil;
import edu.uci.ics.asterix.event.management.EventrixClient;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.error.VerificationUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixRuntimeState;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

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
        InstallerUtil.validateAsterixInstanceNotExists(asterixInstanceName);
        CreateConfig createConfig = (CreateConfig) config;
        cluster = EventUtil.getCluster(createConfig.clusterPath);
        asterixConfiguration = InstallerUtil.getAsterixConfiguration(createConfig.asterixConfPath);
        AsterixInstance asterixInstance = InstallerUtil.createAsterixInstance(asterixInstanceName, cluster,
                asterixConfiguration);
        InstallerUtil.evaluateConflictWithOtherInstances(asterixInstance);
        InstallerUtil.createAsterixZip(asterixInstance);
        InstallerUtil.createClusterProperties(cluster, asterixConfiguration);
        EventrixClient eventrixClient = InstallerUtil.getEventrixClient(cluster);
        PatternCreator pc = new PatternCreator();

        Patterns asterixBinarytrasnferPattern = pc.getAsterixBinaryTransferPattern(asterixInstanceName, cluster);
        eventrixClient.submit(asterixBinarytrasnferPattern);

        Patterns patterns = pc.getStartAsterixPattern(asterixInstanceName, cluster);
        eventrixClient.submit(patterns);

        AsterixRuntimeState runtimeState = VerificationUtil.getAsterixRuntimeState(asterixInstance);
        VerificationUtil.updateInstanceWithRuntimeDescription(asterixInstance, runtimeState, true);
        ServiceProvider.INSTANCE.getLookupService().writeAsterixInstance(asterixInstance);
        InstallerUtil.deleteDirectory(InstallerDriver.getManagixHome() + File.separator + InstallerDriver.ASTERIX_DIR
                + File.separator + asterixInstanceName);
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
