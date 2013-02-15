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

import java.util.Date;
import java.util.Properties;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.installer.driver.ManagixUtil;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.service.ILookupService;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class AlterCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        String instanceName = ((AlterConfig) config).name;
        ManagixUtil.validateAsterixInstanceExists(instanceName, State.INACTIVE);
        ILookupService lookupService = ServiceProvider.INSTANCE.getLookupService();
        AsterixInstance instance = lookupService.getAsterixInstance(instanceName);

        Properties asterixConfProp = ManagixUtil.getAsterixConfiguration(((AlterConfig) config).confPath);
        instance.setConfiguration(asterixConfProp);
        instance.setModifiedTimestamp(new Date());
        lookupService.updateAsterixInstance(instance);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new AlterConfig();
    }

}

class AlterConfig implements CommandConfig {

    @Option(name = "-h", required = false, usage = "Help")
    public boolean help = false;

    @Option(name = "-n", required = false, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-conf", required = false, usage = "Path to instance configuration")
    public String confPath;

}
