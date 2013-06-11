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

import java.util.Date;
import java.util.List;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.model.BackupInfo;
import edu.uci.ics.asterix.installer.schema.conf.Backup;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class BackupCommand extends AbstractCommand {

    public static final String ASTERIX_ROOT_METADATA_DIR = "asterix_root_metadata";

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((BackupConfig) config).name;
        AsterixInstance instance = InstallerUtil.validateAsterixInstanceExists(asterixInstanceName, State.INACTIVE);
        List<BackupInfo> backupInfo = instance.getBackupInfo();
        PatternCreator pc = new PatternCreator();
        Backup backupConf = InstallerDriver.getConfiguration().getBackup();
        Patterns patterns = pc.getBackUpAsterixPattern(instance, backupConf);
        InstallerUtil.getEventrixClient(instance.getCluster()).submit(patterns);
        int backupId = backupInfo.size();
        BackupInfo binfo = new BackupInfo(backupId, new Date(), backupConf);
        backupInfo.add(binfo);
        LOGGER.info(asterixInstanceName + " backed up " + binfo);
        ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(instance);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new BackupConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nThe backup command allows you to take a"
                + "\nbackup of the data stored with an ASTERIX instance. "
                + "\nThe backed up snapshot is stored either in HDFS or on the local file system of each node in the ASTERIX cluster."
                + "\nThe target location of backup can be configured in $MANAGIX_HOME/conf/managix-conf.xml"
                + "\n\nAvailable arguments/options:" + "\n-n name of the Asterix instance";

    }

}

class BackupConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of the Asterix instance")
    public String name;

}
