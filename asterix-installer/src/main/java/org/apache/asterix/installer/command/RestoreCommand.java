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

import java.util.List;

import org.kohsuke.args4j.Option;

import org.apache.asterix.event.model.AsterixInstance;
import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.event.model.BackupInfo;
import org.apache.asterix.event.schema.pattern.Patterns;
import org.apache.asterix.event.service.AsterixEventService;
import org.apache.asterix.event.service.AsterixEventServiceUtil;
import org.apache.asterix.event.util.PatternCreator;
import org.apache.asterix.installer.driver.InstallerDriver;

public class RestoreCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((RestoreConfig) config).name;
        AsterixInstance instance = AsterixEventServiceUtil.validateAsterixInstanceExists(asterixInstanceName,
                State.INACTIVE);
        int backupId = ((RestoreConfig) config).backupId;
        List<BackupInfo> backupInfoList = instance.getBackupInfo();
        if (backupInfoList.size() <= backupId || backupId < 0) {
            throw new IllegalStateException("Invalid backup id");
        }

        BackupInfo backupInfo = backupInfoList.get(backupId);
        Patterns patterns = PatternCreator.INSTANCE.getRestoreAsterixPattern(instance, backupInfo);
        AsterixEventService.getAsterixEventServiceClient(instance.getCluster()).submit(patterns);
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