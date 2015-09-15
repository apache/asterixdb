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

import java.util.Date;
import java.util.List;

import org.kohsuke.args4j.Option;

import org.apache.asterix.event.model.AsterixInstance;
import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.event.model.BackupInfo;
import org.apache.asterix.event.schema.pattern.Patterns;
import org.apache.asterix.event.service.AsterixEventService;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.event.service.ServiceProvider;
import org.apache.asterix.event.util.PatternCreator;
import org.apache.asterix.installer.driver.InstallerDriver;
import org.apache.asterix.installer.schema.conf.Backup;

public class BackupCommand extends AbstractCommand {

    public static final String ASTERIX_ROOT_METADATA_DIR = "asterix_root_metadata";

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((BackupConfig) config).name;
        AsterixInstance instance = AsterixEventServiceUtil.validateAsterixInstanceExists(asterixInstanceName,
                State.INACTIVE);
        List<BackupInfo> backupInfo = instance.getBackupInfo();
        Backup backupConf = AsterixEventService.getConfiguration().getBackup();
        Patterns patterns = PatternCreator.INSTANCE.getBackUpAsterixPattern(instance, backupConf);
        AsterixEventService.getAsterixEventServiceClient(instance.getCluster()).submit(patterns);
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
