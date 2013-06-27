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
package edu.uci.ics.asterix.drivers;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.api.java.AsterixJavaClient;
import edu.uci.ics.asterix.common.config.GlobalConfig;

public class AsterixCLI {
    private static class Options {
        @Option(name = "-properties", usage = "Name of properties file", required = true)
        public String properties;

        @Option(name = "-output", usage = "Output folder to place results", required = true)
        public String outputFolder;

        @Argument(usage = "AQL Files to run", multiValued = true, required = true)
        public List<String> args;
    }

    public static void main(String args[]) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        setUp(options);
        try {
            for (String queryFile : options.args) {
                Reader in = new FileReader(queryFile);
                AsterixJavaClient ajc = new AsterixJavaClient(
                        AsterixHyracksIntegrationUtil.getHyracksClientConnection(), in);
                try {
                    ajc.compile(true, false, false, false, false, true, false);
                } finally {
                    in.close();
                }
                ajc.execute();
            }
        } finally {
            tearDown();
        }
        System.exit(0);
    }

    public static void setUp(Options options) throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, options.properties);
        System.setProperty(GlobalConfig.WEB_SERVER_PORT_PROPERTY, "19002");
        File outdir = new File(options.outputFolder);
        outdir.mkdirs();

        File log = new File("asterix_logs");
        if (log.exists())
            FileUtils.deleteDirectory(log);
        File lsn = new File("last_checkpoint_lsn");
        lsn.deleteOnExit();

        AsterixHyracksIntegrationUtil.init();
    }

    public static void tearDown() throws Exception {
        AsterixHyracksIntegrationUtil.deinit();
    }

}