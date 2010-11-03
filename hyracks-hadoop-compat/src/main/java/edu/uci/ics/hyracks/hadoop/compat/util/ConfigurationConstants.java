package edu.uci.ics.hyracks.hadoop.compat.util;

public class ConfigurationConstants {

    public static final String clusterControllerHost = "clusterControllerHost";
    public static final String namenodeURL = "fs.default.name";
    public static final String dcacheServerConfiguration = "dcacheServerConfiguration";
    public static final String[] systemLibs = new String[] { "hyracksDataflowStdLib", "hyracksDataflowCommonLib",
            "hyracksDataflowHadoopLib", "hadoopCoreLib" };

}
