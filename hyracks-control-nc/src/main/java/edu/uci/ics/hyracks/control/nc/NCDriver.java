/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import org.kohsuke.args4j.CmdLineParser;

import edu.uci.ics.dcache.client.DCacheClient;
import edu.uci.ics.dcache.client.DCacheClientConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;

public class NCDriver {
    public static void main(String args[]) throws Exception {
        NCConfig ncConfig = new NCConfig();
        CmdLineParser cp = new CmdLineParser(ncConfig);
        try {
            cp.parseArgument(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            cp.printUsage(System.err);
            return;
        }

        DCacheClientConfig dccConfig = new DCacheClientConfig();
        dccConfig.servers = ncConfig.dcacheClientServers;
        dccConfig.serverLocal = ncConfig.dcacheClientServerLocal;
        dccConfig.path = ncConfig.dcacheClientPath;

        DCacheClient.get().init(dccConfig);

        final NodeControllerService nService = new NodeControllerService(ncConfig);
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
    }
}