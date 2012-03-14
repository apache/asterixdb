package edu.uci.ics.asterix.drivers;

import java.io.FileReader;

import org.kohsuke.args4j.CmdLineParser;

import edu.uci.ics.asterix.api.common.AsterixClientConfig;
import edu.uci.ics.asterix.api.java.AsterixJavaClient;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;

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
        IHyracksClientConnection hcc = exec ? new HyracksConnection("localhost", acc.hyracksPort) : null;
        AsterixJavaClient q = compileQuery(hcc, acc.getArguments().get(0), new Boolean(acc.optimize), new Boolean(
                acc.onlyPhysical), exec || new Boolean(acc.hyracksJob));
        if (exec) {
            q.execute();
        }
    }

    private static AsterixJavaClient compileQuery(IHyracksClientConnection hcc, String filename, boolean optimize,
            boolean onlyPhysical, boolean createBinaryRuntime) throws Exception {
        FileReader reader = new FileReader(filename);
        AsterixJavaClient q = new AsterixJavaClient(hcc, reader);
        q.compile(optimize, true, true, true, onlyPhysical, createBinaryRuntime, createBinaryRuntime);
        return q;
    }

}