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
import java.util.List;

import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.ManagixUtil;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;
import edu.uci.ics.asterix.installer.model.BackupInfo;
import edu.uci.ics.asterix.installer.service.ServiceProvider;

public class BackupCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        String asterixInstanceName = ((BackupConfig) config).name;
        AsterixInstance instance = ManagixUtil.validateAsterixInstanceExists(asterixInstanceName, State.INACTIVE);
        List<BackupInfo> backupInfo = instance.getBackupInfo();
        PatternCreator pc = new PatternCreator();
        Patterns patterns = pc.getBackUpAsterixPattern(instance, ((BackupConfig) config).localPath);
        ManagixUtil.getEventrixClient(instance.getCluster()).submit(patterns);
        int backupId = backupInfo.size();
        BackupInfo binfo = new BackupInfo(backupId, new Date());
        backupInfo.add(binfo);
        System.out.println(asterixInstanceName + " backed up " + binfo);
        ServiceProvider.INSTANCE.getLookupService().updateAsterixInstance(instance);
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new BackupConfig();
    }

    @Override
    protected String getUsageDescription() {
        // TODO Auto-generated method stub
        return null;
    }

}

class BackupConfig implements CommandConfig {

    @Option(name = "-h", required = false, usage = "Help")
    public boolean help = false;

    @Option(name = "-n", required = true, usage = "Name of the Asterix instance")
    public String name;

    @Option(name = "-local", required = false, usage = "Path on the local file system for backup")
    public String localPath;

}
