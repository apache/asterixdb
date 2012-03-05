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
