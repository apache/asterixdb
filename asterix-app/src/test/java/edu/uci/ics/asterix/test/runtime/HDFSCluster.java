package edu.uci.ics.asterix.test.runtime;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class HDFSCluster {

    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final int nameNodePort = 10009;
    private static final String DATA_PATH = "data/tpch0.001";
    private static final String HDFS_PATH = "/tpch";
    private static final HDFSCluster INSTANCE = new HDFSCluster();

    private MiniDFSCluster dfsCluster;
    private int numDataNodes = 2;
    private JobConf conf = new JobConf();

    public static HDFSCluster getInstance() {
        return INSTANCE;
    }

    private HDFSCluster() {

    }

    public void setup() throws Exception {
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        cleanupLocal();
        dfsCluster = new MiniDFSCluster(nameNodePort, conf, numDataNodes, true, true, StartupOption.REGULAR, null);
        FileSystem dfs = FileSystem.get(conf);
        Path src = new Path(DATA_PATH);
        Path dest = new Path(HDFS_PATH);
        dfs.copyFromLocalFile(src, dest);
    }

    private void cleanupLocal() throws IOException {
        // cleanup artifacts created on the local file system
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
    }

    public void cleanup() throws Exception {
        dfsCluster.shutdown();
        cleanupLocal();
    }

    public static void main(String[] args) throws Exception {
        HDFSCluster cluster = new HDFSCluster();
        cluster.setup();
        cluster.cleanup();
    }

}
