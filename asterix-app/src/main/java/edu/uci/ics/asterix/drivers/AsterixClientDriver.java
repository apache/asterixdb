package edu.uci.ics.asterix.drivers;

import java.io.FileReader;

import org.kohsuke.args4j.CmdLineParser;

import edu.uci.ics.asterix.api.common.AsterixClientConfig;
import edu.uci.ics.asterix.api.java.AsterixJavaClient;

public class AsterixClientDriver {

    public static void main(String args[]) throws Exception {
        AsterixClientConfig acc = new AsterixClientConfig();
        CmdLineParser cmdParser = new CmdLineParser(acc);
        try {
            cmdParser.parseArgument(args);
        } catch (Exception e) {
            cmdParser.printUsage(System.err);
            throw e;
        }

        if (acc.getArguments().isEmpty()) {
            System.err.println("Please specify the file containing the query.");
            return;
        }
        if (acc.getArguments().size() > 1) {
            System.err.print("Too many arguments. ");
            System.err.println("Only the file contained the query needs to be specified.");
            return;
        }
        boolean exec = new Boolean(acc.execute);
        AsterixJavaClient q = compileQuery(acc.getArguments().get(0), new Boolean(acc.optimize), new Boolean(
                acc.onlyPhysical), exec || new Boolean(acc.hyracksJob));
        if (exec) {
            q.execute(acc.hyracksPort);
        }
    }

    private static AsterixJavaClient compileQuery(String filename, boolean optimize, boolean onlyPhysical,
            boolean createBinaryRuntime) throws Exception {
        FileReader reader = new FileReader(filename);
        AsterixJavaClient q = new AsterixJavaClient(reader);
        q.compile(optimize, true, true, true, onlyPhysical, createBinaryRuntime, createBinaryRuntime);
        return q;
    }

}