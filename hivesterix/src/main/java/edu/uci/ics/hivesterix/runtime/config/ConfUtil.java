package edu.uci.ics.hivesterix.runtime.config;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.topology.ClusterTopology;

public class ConfUtil {

    private static JobConf job;
    private static HiveConf hconf;
    private static String[] NCs;
    private static Map<String, List<String>> ncMapping;
    private static IHyracksClientConnection hcc = null;
    private static ClusterTopology topology = null;

    public static JobConf getJobConf(Class<? extends InputFormat> format, Path path) {
        JobConf conf = new JobConf();
        if (job != null)
            conf = job;

        String hadoopPath = System.getProperty("HADOOP_HOME", "/hadoop");
        Path pathCore = new Path(hadoopPath + "/conf/core-site.xml");
        conf.addResource(pathCore);
        Path pathMapRed = new Path(hadoopPath + "/conf/mapred-site.xml");
        conf.addResource(pathMapRed);
        Path pathHDFS = new Path(hadoopPath + "/conf/hdfs-site.xml");
        conf.addResource(pathHDFS);

        conf.setInputFormat(format);
        FileInputFormat.setInputPaths(conf, path);
        return conf;
    }

    public static JobConf getJobConf() {
        JobConf conf = new JobConf();
        if (job != null)
            conf = job;

        String hadoopPath = System.getProperty("HADOOP_HOME", "/hadoop");
        Path pathCore = new Path(hadoopPath + "/conf/core-site.xml");
        conf.addResource(pathCore);
        Path pathMapRed = new Path(hadoopPath + "/conf/mapred-site.xml");
        conf.addResource(pathMapRed);
        Path pathHDFS = new Path(hadoopPath + "/conf/hdfs-site.xml");
        conf.addResource(pathHDFS);

        return conf;
    }

    public static void setJobConf(JobConf conf) {
        job = conf;
    }

    public static void setHiveConf(HiveConf hiveConf) {
        hconf = hiveConf;
    }

    public static HiveConf getHiveConf() {
        if (hconf == null) {
            hconf = new HiveConf(SessionState.class);
            hconf.addResource(new Path("conf/hive-default.xml"));
        }
        return hconf;
    }

    public static String[] getNCs() throws AlgebricksException {
        if (NCs == null) {
            try {
                loadClusterConfig();
            } catch (Exception e) {
                throw new AlgebricksException(e);
            }
        }
        return NCs;
    }

    public static Map<String, List<String>> getNCMapping() throws AlgebricksException {
        if (ncMapping == null) {
            try {
                loadClusterConfig();
            } catch (Exception e) {
                throw new AlgebricksException(e);
            }
        }
        return ncMapping;
    }

    private static void loadClusterConfig() {
        try {
            getHiveConf();
            String ipAddress = hconf.get("hive.hyracks.host");
            int port = Integer.parseInt(hconf.get("hive.hyracks.port"));
            int mpl = Integer.parseInt(hconf.get("hive.hyracks.parrallelism"));
            hcc = new HyracksConnection(ipAddress, port);
            topology = hcc.getClusterTopology();
            Map<String, NodeControllerInfo> ncNameToNcInfos = hcc.getNodeControllerInfos();
            NCs = new String[ncNameToNcInfos.size() * mpl];
            ncMapping = new HashMap<String, List<String>>();
            int i = 0;
            for (Map.Entry<String, NodeControllerInfo> entry : ncNameToNcInfos.entrySet()) {
                String ipAddr = InetAddress.getByAddress(entry.getValue().getNetworkAddress().getIpAddress())
                        .getHostAddress();
                List<String> matchedNCs = ncMapping.get(ipAddr);
                if (matchedNCs == null) {
                    matchedNCs = new ArrayList<String>();
                    ncMapping.put(ipAddr, matchedNCs);
                }
                matchedNCs.add(entry.getKey());
                for (int j = i * mpl; j < i * mpl + mpl; j++)
                    NCs[j] = entry.getKey();
                i++;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static ClusterTopology getClusterTopology() {
        if (topology == null)
            loadClusterConfig();
        return topology;
    }
}
