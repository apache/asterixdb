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
package org.apache.asterix.test.runtime;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.mapred.JobConf;

/**
 * Manages a Mini (local VM) HDFS cluster with a configured number of datanodes.
 *
 * @author ramangrover29
 */
public class HDFSCluster {

    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final int nameNodePort = 31888;
    private static final String DATA_PATH = "data/hdfs";
    private static final String HDFS_PATH = "/asterix";
    private static final HDFSCluster INSTANCE = new HDFSCluster();

    private MiniDFSCluster dfsCluster;
    private int numDataNodes = 2;
    private JobConf conf = new JobConf();
    private FileSystem dfs;

    public static HDFSCluster getInstance() {
        return INSTANCE;
    }

    private HDFSCluster() {

    }

    /**
     * Instantiates the (Mini) DFS Cluster with the configured number of datanodes.
     * Post instantiation, data is laoded to HDFS.
     * Called prior to running the Runtime test suite.
     */
    public void setup() throws Exception {
        setup("");
    }

    public void setup(String basePath) throws Exception {
        conf.addResource(new Path(basePath + PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(basePath + PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(basePath + PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        cleanupLocal();
        //this constructor is deprecated in hadoop 2x
        //dfsCluster = new MiniDFSCluster(nameNodePort, conf, numDataNodes, true, true, StartupOption.REGULAR, null);
        MiniDFSCluster.Builder build = new MiniDFSCluster.Builder(conf);
        build.nameNodePort(nameNodePort);
        build.numDataNodes(numDataNodes);
        build.startupOption(StartupOption.REGULAR);
        dfsCluster = build.build();
        dfs = FileSystem.get(conf);
        loadData(basePath);
    }

    private void loadData(String localDataRoot) throws IOException {
        Path destDir = new Path(HDFS_PATH);
        dfs.mkdirs(destDir);
        File srcDir = new File(localDataRoot + DATA_PATH);
        File[] listOfFiles = srcDir.listFiles();
        for (File srcFile : listOfFiles) {
            Path path = new Path(srcFile.getAbsolutePath());
            dfs.copyFromLocalFile(path, destDir);
        }
    }

    private void cleanupLocal() throws IOException {
        // cleanup artifacts created on the local file system
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
    }

    public void cleanup() throws Exception {
        if (dfsCluster != null) {
            dfsCluster.shutdown();
            cleanupLocal();
        }
    }

}
