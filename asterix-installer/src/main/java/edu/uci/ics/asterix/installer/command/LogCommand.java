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
import java.io.FilenameFilter;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.event.management.EventrixClient;
import edu.uci.ics.asterix.event.schema.pattern.Patterns;
import edu.uci.ics.asterix.installer.driver.InstallerDriver;
import edu.uci.ics.asterix.installer.driver.InstallerUtil;
import edu.uci.ics.asterix.installer.error.InstallerException;
import edu.uci.ics.asterix.installer.events.PatternCreator;
import edu.uci.ics.asterix.installer.model.AsterixInstance;
import edu.uci.ics.asterix.installer.model.AsterixInstance.State;

public class LogCommand extends AbstractCommand {

    @Override
    protected void execCommand() throws Exception {
        InstallerDriver.initConfig(true);
        String asterixInstanceName = ((LogConfig) config).name;
        AsterixInstance instance = InstallerUtil.validateAsterixInstanceExists(asterixInstanceName, State.INACTIVE,
                State.UNUSABLE, State.ACTIVE);
        PatternCreator pc = new PatternCreator();
        EventrixClient client = InstallerUtil.getEventrixClient(instance.getCluster());
        String outputDir = ((LogConfig) config).outputDir == null ? InstallerDriver.getManagixHome() + File.separator + "logdump"
                : ((LogConfig) config).outputDir;
        File f = new File(outputDir);
        String outputDirPath = f.getAbsolutePath();
        if (!f.exists()) {
            boolean success = f.mkdirs();
            if (!success) {
                throw new InstallerException("Unable to create output directory:" + outputDirPath);
            }
        }
        Patterns transferLogPattern = pc.getGenerateLogPattern(asterixInstanceName, instance.getCluster(), outputDirPath);
        client.submit(transferLogPattern);
        File outputDirFile = new File(outputDirPath);
        final String destFileName = "log_" + new Date().toString().replace(' ', '_') + ".zip";
        File destFile = new File(outputDirFile, destFileName);
        InstallerUtil.zipDir(outputDirFile, destFile);

        String[] filesToDelete = outputDirFile.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return !name.equals(destFileName);
            }

        });
        for (String fileS : filesToDelete) {
             f = new File(outputDirFile, fileS);
            if (f.isDirectory()) {
                FileUtils.deleteDirectory(f);
            } else {
                f.delete();
            }
        }
        LOGGER.info("Log zip archive created at " + destFile.getAbsolutePath());
    }

    @Override
    protected CommandConfig getCommandConfig() {
        return new LogConfig();
    }

    @Override
    protected String getUsageDescription() {
        return "\nCreates a tar ball containing log files corresponding to each worker node (NC) and the master (CC)  for an ASTERIX instance"
                + "\n\nAvailable arguments/options"
                + "\n-n name of the ASTERIX instance. \n-d destination directory for producing the tar ball (defaults to) "
                + InstallerDriver.getManagixHome();
    }
}

class LogConfig extends CommandConfig {

    @Option(name = "-n", required = true, usage = "Name of Asterix Instance")
    public String name;

    @Option(name = "-d", required = false, usage = "Destination directory for producing log tar ball")
    public String outputDir;

}
