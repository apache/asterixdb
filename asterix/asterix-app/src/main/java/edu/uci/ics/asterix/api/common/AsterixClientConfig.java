package edu.uci.ics.asterix.api.common;

import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class AsterixClientConfig {
    @Option(name = "-optimize", usage = "Turns compiler optimizations on (if set to true) or off (if set to false). It is true by default.")
    public String optimize = "true";

    @Option(name = "-only-physical", usage = "Prints only the physical annotations, not the entire operators. It is false by default.")
    public String onlyPhysical = "false";

    @Option(name = "-execute", usage = "Executes the job produced by the compiler. It is false by default.")
    public String execute = "false";

    @Option(name = "-hyracks-job", usage = "Generates and prints the Hyracks job. It is false by default.")
    public String hyracksJob = "false";

    @Option(name = "-hyracks-port", usage = "The port used to connect to the Hyracks server.")
    public int hyracksPort = AsterixHyracksIntegrationUtil.DEFAULT_HYRACKS_CC_CLIENT_PORT;

    @Argument
    private List<String> arguments = new ArrayList<String>();

    public List<String> getArguments() {
        return arguments;
    }
}
