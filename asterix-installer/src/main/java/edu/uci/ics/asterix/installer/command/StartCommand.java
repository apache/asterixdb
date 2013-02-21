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

import java.io.File;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.ManagixDriver;
import edu.uci.ics.asterix.installer.driver.ManagixUtil;
import edu.uci.ics.asterix.installer.error.VerificationUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.model.AsterixRuntimeState;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class StartCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        String asterixInstanceName = ((StartConfig) config).name;
        AsterixInstance instance = ManagixUtil.validateAsterixInstanceExists(asterixInstanceName, State.INACTIVE);
        ManagixUtil.createAsterixZip(instance, false);
        PatternCreator pc = new PatternCreator();
        Patterns patterns = pc.getStartAsterixPattern(asterixInstanceName, instance.getCluster());
        ManagixUtil.getEventrixClient(instance.getCluster()).submit(patterns);
        ManagixUtil.deleteDirectory(ManagixDriver.getManagixHome() + File.separator + ManagixDriver.ASTERIX_DIR
                + File.separator + asterixInstanceName);
        AsterixRuntimeState runtimeState = VerificationUtil.getAsterixRuntimeState(instance);
        VerificationUtil.updateInstanceWithRuntimeDescription(instance, runtimeState, true);
        System.out.println(instance.getDescription(false));
        ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(instance);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new StartConfig();
    }

    @Override
    protected String getUsageDescription() {
        // TODO Auto-generated method stub
        return null;
    }

}

class StartConfig implements CommandConfig {

    @Option(name = "-h", required = false, usage = "Help")
    public boolean help = false;

    @Option(name = "-n", required = false, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-conf", required = false, usage = "Path to instance configuration")
    public String confPath;

}
