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
package edu.uci.ics.asterix.tools.datagen;

import java.io.File;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;

public class AdgClientDriver {

    public static final int NUMBER_OF_ARGUMENTS = 2;

    public static class AdgClientConfig {

        @Argument(index = 0, required = true, metaVar = "ARG1", usage = "The file containing the annotated schema.")
        private File schemaFile;

        @Argument(index = 1, required = true, metaVar = "ARG2", usage = "The output directory path.")
        private File outputDir;
    }

    public static void main(String[] args) throws Exception {
        AdgClientConfig acc = new AdgClientConfig();
        CmdLineParser cmdParser = new CmdLineParser(acc);
        try {
            cmdParser.parseArgument(args);
        } catch (Exception e) {
            cmdParser.printUsage(System.err);
            throw e;
        }
        AdmDataGen adg = new AdmDataGen(acc.schemaFile, acc.outputDir);
        adg.init();
        adg.dataGen();
    }
}
