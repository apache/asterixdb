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
package edu.uci.ics.asterix.event.service;

import java.io.File;
import java.io.FileFilter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.uci.ics.asterix.event.error.OutputHandler;
import edu.uci.ics.asterix.event.management.AsterixEventServiceClient;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.installer.schema.conf.Configuration;

public class AsterixEventService {

    private static final Logger LOGGER = Logger.getLogger(AsterixEventService.class.getName());
    private static Configuration configuration;
    private static String asterixDir;
    private static String asterixZip;
    private static String eventHome;

    public static void initialize(Configuration configuration, String asterixDir, String eventHome) throws Exception {
        AsterixEventService.configuration = configuration;
        AsterixEventService.asterixDir = asterixDir;
        AsterixEventService.asterixZip = initBinary("asterix-server");
        AsterixEventService.eventHome = eventHome;
    }

    public static AsterixEventServiceClient getAsterixEventServiceClient(Cluster cluster, boolean transferArtifacts,
            boolean dryRun) throws Exception {
        AsterixEventServiceClient client = new AsterixEventServiceClient(configuration, eventHome, cluster,
                transferArtifacts, dryRun, OutputHandler.INSTANCE);
        return client;
    }

    public static AsterixEventServiceClient getAsterixEventServiceClient(Cluster cluster) throws Exception {
        AsterixEventServiceClient client = new AsterixEventServiceClient(configuration, eventHome, cluster, false,
                false, OutputHandler.INSTANCE);
        return client;
    }

    private static String initBinary(final String fileNamePattern) {
        File file = new File(asterixDir);
        File[] zipFiles = file.listFiles(new FileFilter() {
            public boolean accept(File arg0) {
                return arg0.getAbsolutePath().contains(fileNamePattern) && arg0.isFile();
            }
        });
        if (zipFiles.length == 0) {
            String msg = " Binary not found at " + asterixDir;
            LOGGER.log(Level.FATAL, msg);
            throw new IllegalStateException(msg);
        }
        if (zipFiles.length > 1) {
            String msg = " Multiple binaries found at " + asterixDir;
            LOGGER.log(Level.FATAL, msg);
            throw new IllegalStateException(msg);
        }

        return zipFiles[0].getAbsolutePath();
    }

    public static Configuration getConfiguration() {
        return configuration;
    }

    public static String getAsterixZip() {
        return asterixZip;
    }

    public static String getAsterixDir() {
        return asterixDir;
    }

    public static String getEventHome() {
        return eventHome;
    }
}
