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

import java.util.List;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.model.BackupInfo;

public class RestoreCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((RestoreConfig) config).name;
        AsterixInstance instance = InstallerUtil.validateAsterixInstanceExists(asterixInstanceName, State.INACTIVE);
        int backupId = ((RestoreConfig) config).backupId;
        List<BackupInfo> backupInfoList = instance.getBackupInfo();
        if (backupInfoList.size() <= backupId || backupId < 0) {
            throw new IllegalStateException("Invalid backup id");
        }

        BackupInfo backupInfo = backupInfoList.get(backupId);
        PatternCreator pc = new PatternCreator();
        Patterns patterns = pc.getRestoreAsterixPattern(instance, backupInfo);
        InstallerUtil.getEventrixClient(instance.getCluster()).submit(patterns);
        LOGGER.info("Asterix instance: " + asterixInstanceName + " has been restored from backup");
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new RestoreConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nRestores an ASTERIX instance's data from a previously taken backup snapshot."
                + "\n\nAvailable arguments/options" + "\n-n name of the ASTERIX instance"
                + "\n-b id of the backup snapshot ";
    }

}

class RestoreConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of the Asterix instance")
    public String name;

    @Option(name = "-b", required = true, usage = "Id corresponding to the backed up version")
    public int backupId;

}