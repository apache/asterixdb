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
package edu.uci.ics.hyracks.control.nc;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.kohsuke.args4j.CmdLineParser;

import edu.uci.ics.hyracks.api.lifecycle.LifeCycleComponentManager;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;

public class NCDriver {
    private static final Logger LOGGER = Logger.getLogger(NCDriver.class.getName());

    public static void main(String args[]) throws Exception {
        try {
            NCConfig ncConfig = new NCConfig();
            CmdLineParser cp = new CmdLineParser(ncConfig);
            try {
                cp.parseArgument(args);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                cp.printUsage(System.err);
                return;
            }

            final NodeControllerService nService = new NodeControllerService(ncConfig);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.severe("Setting uncaught exception handler " + LifeCycleComponentManager.INSTANCE);
            }
            Thread.currentThread().setUncaughtExceptionHandler(LifeCycleComponentManager.INSTANCE);
            nService.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        nService.stop();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            while (true) {
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}