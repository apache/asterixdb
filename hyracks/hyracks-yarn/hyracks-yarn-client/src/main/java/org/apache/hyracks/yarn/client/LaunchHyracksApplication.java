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
package edu.uci.ics.hyracks.yarn.client;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.yarn.common.protocols.clientrm.YarnApplication;
import edu.uci.ics.hyracks.yarn.common.protocols.clientrm.YarnClientRMConnection;
import edu.uci.ics.hyracks.yarn.common.resources.LocalResourceHelper;
import edu.uci.ics.hyracks.yarn.common.resources.ResourceHelper;

public class LaunchHyracksApplication {
    private final Options options;

    private LaunchHyracksApplication(Options options) {
        this.options = options;
    }

    private void run() throws Exception {
        Configuration conf = new Configuration();
        YarnConfiguration yconf = new YarnConfiguration(conf);
        YarnClientRMConnection crmc = new YarnClientRMConnection(yconf);

        YarnApplication app = crmc.createApplication(options.appName);

        ContainerLaunchContext clCtx = app.getContainerLaunchContext();

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        File amZipFile = new File(System.getProperty("basedir") + "/hyracks-yarn-am/hyracks-yarn-am.zip");
        localResources.put("archive", LocalResourceHelper.createArchiveResource(conf, amZipFile));
        clCtx.setLocalResources(localResources);

        String command = "./archive/bin/hyracks-yarn-am 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

        List<String> commands = new ArrayList<String>();
        commands.add(command);
        clCtx.setCommands(commands);

        clCtx.setResource(ResourceHelper.createMemoryCapability(options.amMemory));

        app.submit();
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        try {
            parser.parseArgument(args);
        } catch (Exception e) {
            parser.printUsage(System.err);
            return;
        }
        new LaunchHyracksApplication(options).run();
    }

    private static class Options {
        @Option(name = "-application-name", required = true, usage = "Application Name")
        String appName;

        @Option(name = "-am-host", required = false, usage = "Application master host name (default: *). Currently has NO effect")
        String amHostName = "*";

        @Option(name = "-am-memory", required = false, usage = "Application Master memory requirements")
        int amMemory = 128;

        @Option(name = "-workers", required = true, usage = "Number of worker containers")
        int nWorkers;

        @Option(name = "-worker-memory", required = true, usage = "Amount of memory to provide to each worker")
        int workerMemory;

        @Option(name = "-extra-jars", required = false, usage = "Other jars that need to be added to the classpath")
        String extraJars = "";
    }
}