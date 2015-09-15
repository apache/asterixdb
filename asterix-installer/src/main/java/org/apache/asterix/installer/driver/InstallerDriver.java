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
package org.apache.asterix.installer.driver;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;

import org.apache.asterix.event.service.AsterixEventService;
import org.apache.asterix.event.service.ILookupService;
import org.apache.asterix.event.service.ServiceProvider;
import org.apache.asterix.installer.command.CommandHandler;
import org.apache.asterix.installer.command.ConfigureCommand;
import org.apache.asterix.installer.schema.conf.Configuration;

public class InstallerDriver {

    private static final Logger LOGGER = Logger.getLogger(InstallerDriver.class.getName());

    public static final String MANAGIX_INTERNAL_DIR = ".installer";
    public static final String ENV_MANAGIX_HOME = "MANAGIX_HOME";
    public static final String MANAGIX_CONF_XML = "conf" + File.separator + "managix-conf.xml";
    public static final String ASTERIX_DIR = "asterix";

    private static String managixHome;

    public static void initConfig(boolean ensureLookupServiceIsRunning) throws Exception {
        File configFile = new File(managixHome + File.separator + MANAGIX_CONF_XML);
        JAXBContext configCtx = JAXBContext.newInstance(Configuration.class);
        Unmarshaller unmarshaller = configCtx.createUnmarshaller();
        Configuration conf = (Configuration) unmarshaller.unmarshal(configFile);
        String asterixDir = managixHome + File.separator + ASTERIX_DIR;
        String eventHome = managixHome + File.separator + MANAGIX_INTERNAL_DIR;
        AsterixEventService.initialize(conf, asterixDir, eventHome);

        ILookupService lookupService = ServiceProvider.INSTANCE.getLookupService();
        if (ensureLookupServiceIsRunning) {
            if (!conf.isConfigured()) {
                try {
                    configure();
                    /* read back the configuration file updated as part of configure command*/
                    conf = (Configuration) unmarshaller.unmarshal(configFile);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (!lookupService.isRunning(conf)) {
                lookupService.startService(conf);
            }
        }

    }

    private static void configure() throws Exception {
        ConfigureCommand cmd = new ConfigureCommand();
        cmd.execute(new String[] { "configure" });
    }

    public static String getManagixHome() {
        return managixHome;
    }

    public static void setManagixHome(String managixHome) {
        InstallerDriver.managixHome = managixHome;
    }

    public static void main(String args[]) {
        try {
            if (args.length != 0) {
                managixHome = System.getenv(ENV_MANAGIX_HOME);
                CommandHandler cmdHandler = new CommandHandler();
                cmdHandler.processCommand(args);
            } else {
                printUsage();
            }
        } catch (IllegalArgumentException iae) {
            LOGGER.error("Unknown command");
            printUsage();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            if (e.getMessage() == null || e.getMessage().length() < 10) {
                // less than 10 characters of error message is probably not enough
                e.printStackTrace();
            }
        }
    }

    private static void printUsage() {
        StringBuffer buffer = new StringBuffer("managix <command> <options>" + "\n");
        buffer.append("Commands" + "\n");
        buffer.append("create   " + ":" + " Creates a new asterix instance" + "\n");
        buffer.append("delete   " + ":" + " Deletes an asterix instance" + "\n");
        buffer.append("start    " + ":" + " Starts an  asterix instance" + "\n");
        buffer.append("stop     " + ":" + " Stops an asterix instance that is in ACTIVE state" + "\n");
        buffer.append("backup   " + ":" + " Creates a back up for an existing asterix instance" + "\n");
        buffer.append("restore  " + ":" + " Restores an asterix instance" + "\n");
        buffer.append("alter    " + ":" + " Alter the instance's configuration settings" + "\n");
        buffer.append("describe " + ":" + " Describes an existing asterix instance" + "\n");
        buffer.append("validate " + ":" + " Validates the installer/cluster configuration" + "\n");
        buffer.append("configure" + ":" + " Auto-generate configuration for local psedu-distributed Asterix instance"
                + "\n");
        buffer.append("install  " + ":" + " Installs a library to an asterix instance" + "\n");
        buffer.append("uninstall" + ":" + " Uninstalls a library from an asterix instance" + "\n");
        buffer.append("log      " + ":"
                + " Produce a tar archive contianing log files from the master and worker nodes" + "\n");
        buffer.append("shutdown " + ":" + " Shutdown the installer service" + "\n");
        buffer.append("help     " + ":" + " Provides usage description of a command" + "\n");
        buffer.append("version  " + ":" + " Provides version of Asterix/Managix" + "\n");

        buffer.append("\nTo get more information about a command, use managix help -cmd <command>");
        LOGGER.info(buffer.toString());
    }

}
