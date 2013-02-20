/*
 * Copyright 2009-2012 by The Regents of the University of California
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

import java.util.List;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.installer.driver.ManagixUtil;
import edu.uci.ics.asterix.installer.error.ManagixException;
import edu.uci.ics.asterix.installer.error.VerificationUtil;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.model.AsterixRuntimeState;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class DescribeCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        String asterixInstanceName = ((DescribeConfig) config).name;
        boolean adminView = ((DescribeConfig) config).admin;
        if (asterixInstanceName != null) {
            ManagixUtil
                    .validateAsterixInstanceExists(asterixInstanceName, State.INACTIVE, State.ACTIVE, State.UNUSABLE);
            AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(
                    asterixInstanceName);
            if (instance != null) {
                AsterixRuntimeState state = VerificationUtil.getAsterixRuntimeState(instance);
                boolean expectedRunning = instance.getState().equals(State.UNUSABLE) ? instance.getPreviousState()
                        .equals(State.ACTIVE) : !instance.getState().equals(State.INACTIVE);
                VerificationUtil.updateInstanceWithRuntimeDescription(instance, state, expectedRunning);
                ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(instance);
                System.out.println(instance.getDescription(adminView));
            } else {
                throw new ManagixException("Asterix instance by the name " + asterixInstanceName + " does not exist.");
            }
        } else {
            List<AsterixInstance> asterixInstances = ServiceProvider.INSTANCE.getLookupService().getAsterixInstances();
            if (asterixInstances.size() > 0) {
                for (AsterixInstance instance : asterixInstances) {
                    AsterixRuntimeState state = VerificationUtil.getAsterixRuntimeState(instance);
                    boolean expectedRunning = instance.getState().equals(State.UNUSABLE) ? instance.getPreviousState()
                            .equals(State.ACTIVE) : !instance.getState().equals(State.INACTIVE);
                    VerificationUtil.updateInstanceWithRuntimeDescription(instance, state, expectedRunning);
                    ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(instance);
                    System.out.println(instance.getDescription(adminView));
                }
            } else {
                LOGGER.info("No Asterix instances found!");
            }

        }
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new DescribeConfig();
    }

    @Override
    protected String getUsageDescription() {
        // TODO Auto-generated method stub
        return null;
    }

}

class DescribeConfig implements CommandConfig {

    @Option(name = "-h", required = false, usage = "Help")
    public boolean help = false;

    @Option(name = "-n", required = false, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-admin", required = false, usage = "Detailed description")
    public boolean admin;

}
