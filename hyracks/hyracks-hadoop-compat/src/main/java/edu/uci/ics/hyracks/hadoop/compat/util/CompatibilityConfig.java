package edu.uci.ics.hyracks.hadoop.compat.util;

import org.kohsuke.args4j.Option;

public class CompatibilityConfig {

    @Option(name = "-cluster", required = true, usage = "Defines the path to the configuration file that provides the following info: +"
            + " (1) Address of HyracksClusterController service" + " (2) Address of Hadoop namenode service")
    public String clusterConf;

    @Option(name = "-jobFiles", usage = "Comma separated list of jobFiles. "
            + "Each job file defines the hadoop job + " + "The order in the list defines the sequence in which"
            + "the jobs are to be executed")
    public String jobFiles;

    @Option(name = "-userLibs", usage = " A comma separated list of jar files that are required to be addedd to classpath when running "
            + " mappers/reducers etc ")
    public String userLibs;

}
