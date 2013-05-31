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
package edu.uci.ics.asterix.installer.driver;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;

import edu.uci.ics.asterix.event.service.AsterixEventService;
import edu.uci.ics.asterix.event.service.ILookupService;
import edu.uci.ics.asterix.event.service.ServiceProvider;
import edu.uci.ics.asterix.installer.command.CommandHandler;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;

public class InstallerDriver {

    private static final Logger LOGGER = Logger.getLogger(InstallerDriver.class.getName());

    public static final String MANAGIX_INTERNAL_DIR = ".installer";
    public static final String ENV_MANAGIX_HOME = "MANAGIX_HOME";
    public static final String MANAGIX_CONF_XML = "conf" + File.separator + "managix-conf.xml";
    public static final String ASTERIX_DIR = "asterix";

    private static String managixHome;

    public static void initConfig() throws Exception {
        File configFile = new File(managixHome + File.separator + MANAGIX_CONF_XML);
        JAXBContext configCtx = JAXBContext.newInstance(Configuration.class);
        Unmarshaller unmarshaller = configCtx.createUnmarshaller();
        Configuration conf = (Configuration) unmarshaller.unmarshal(configFile);
        String asterixDir = managixHome + File.separator + ASTERIX_DIR;
        String eventHome = managixHome + File.separator + MANAGIX_INTERNAL_DIR;
        AsterixEventService.initialize(conf, asterixDir, eventHome);

        ILookupService lookupService = ServiceProvider.INSTANCE.getLookupService();
        if (!lookupService.isRunning(conf)) {
            lookupService.startService(conf);
        }
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
            if (e.getMessage() == null || e.getMessage().length() == 0) {
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
        buffer.append("configure" + ":" + " Configure the Asterix installer" + "\n");
        buffer.append("shutdown " + ":" + " Shutdown the installer service" + "\n");
        buffer.append("help     " + ":" + " Provides usage description of a command" + "\n");
        buffer.append("\nTo get more information about a command, use managix help -cmd <command>");
        LOGGER.info(buffer.toString());
    }
}
