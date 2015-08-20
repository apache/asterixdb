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
package edu.uci.ics.hyracks.control.cc;

import org.kohsuke.args4j.CmdLineParser;

import edu.uci.ics.hyracks.control.common.controllers.CCConfig;

public class CCDriver {
    public static void main(String args[]) throws Exception {
        try {
            CCConfig ccConfig = new CCConfig();
            CmdLineParser cp = new CmdLineParser(ccConfig);
            try {
                cp.parseArgument(args);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                cp.printUsage(System.err);
                return;
            }
            ClusterControllerService ccService = new ClusterControllerService(ccConfig);
            ccService.start();
            while (true) {
                Thread.sleep(100000);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}