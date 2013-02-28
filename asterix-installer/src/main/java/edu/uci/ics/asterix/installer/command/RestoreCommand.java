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

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;

public class RestoreCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        String asterixInstanceName = ((RestoreConfig) config).name;
        AsterixInstance instance = InstallerUtil.validateAsterixInstanceExists(asterixInstanceName, State.INACTIVE);
        int backupId = ((RestoreConfig) config).backupId;
        if (instance.getBackupInfo().size() <= backupId || backupId < 0) {
            throw new IllegalStateException("Invalid backup id");
        }
        PatternCreator pc = new PatternCreator();
        Patterns patterns = pc.getRestoreAsterixPattern(instance, backupId);
        InstallerUtil.getEventrixClient(instance.getCluster()).submit(patterns);
        LOGGER.info("Asterix instance: " + asterixInstanceName + " has been restored from backup");
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new RestoreConfig();
    }

    @Override
    protected String getUsageDescription() {
        // TODO Auto-generated method stub
        return null;
    }

}

class RestoreConfig implements CommandConfig {

    @Option(name = "-h", required = false, usage = "Help")
    public boolean help = false;

    @Option(name = "-n", required = true, usage = "Name of the Asterix instance")
    public String name;

    @Option(name = "-b", required = true, usage = "Id corresponding to the backed up version")
    public int backupId;

}