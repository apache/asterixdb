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
package org.apache.asterix.drivers;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.List;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.api.java.AsterixJavaClient;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

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
        ILangCompilationProvider compilationProvider = new AqlCompilationProvider();
        setUp(options);
        try {
            for (String queryFile : options.args) {
                Reader in = new FileReader(queryFile);
                AsterixJavaClient ajc = new AsterixJavaClient(
                        AsterixHyracksIntegrationUtil.getHyracksClientConnection(), in, compilationProvider);
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
        File outdir = new File(options.outputFolder);
        outdir.mkdirs();

        File log = new File("asterix_logs");
        if (log.exists())
            FileUtils.deleteDirectory(log);
        File lsn = new File("last_checkpoint_lsn");
        lsn.deleteOnExit();

        AsterixHyracksIntegrationUtil.init(false);
    }

    public static void tearDown() throws Exception {
        AsterixHyracksIntegrationUtil.deinit(false);
    }

}