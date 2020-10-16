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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.asterix.external.dataset.adapter.GenericAdapter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hyracks.api.util.IoUtil;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.filesystem.JSONFileReader;

import parquet.avro.AvroParquetWriter;

/**
 * Manages a Mini (local VM) HDFS cluster with a configured number of datanodes.
 */
public class HDFSCluster {

    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final String MINIDFS_BASEDIR = "target" + File.separatorChar + "hdfs";
    private static final int nameNodePort = 31888;
    private static final String DATA_PATH = "data/hdfs";
    private static final String HDFS_PATH = "/asterix";
    private static final HDFSCluster INSTANCE = new HDFSCluster();
    //Temporary folder that holds generated binary files
    private static final String BINARY_GEN_BASEDIR = "target" + File.separatorChar + "generated_bin_files";
    //How many records should the schema inference method inspect to infer the schema for parquet files
    private static final int NUM_OF_RECORDS_SCHEMA = 20;

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
     * Post instantiation, data is loaded to HDFS.
     * Called prior to running the Runtime test suite.
     */
    public void setup() throws Exception {
        setup(new File("."));
    }

    public void setup(File basePath) throws Exception {
        File hadoopConfDir = new File(basePath, PATH_TO_HADOOP_CONF);
        conf.addResource(new Path(new File(hadoopConfDir, "core-site.xml").getPath()));
        conf.addResource(new Path(new File(hadoopConfDir, "mapred-site.xml").getPath()));
        conf.addResource(new Path(new File(hadoopConfDir, "hdfs-site.xml").getPath()));
        cleanupLocal();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, MINIDFS_BASEDIR);
        MiniDFSCluster.Builder build = new MiniDFSCluster.Builder(conf);
        build.nameNodePort(nameNodePort);
        build.numDataNodes(numDataNodes);
        build.startupOption(StartupOption.REGULAR);
        dfsCluster = build.build();
        dfs = FileSystem.get(conf);
        //Generate binary files from JSON files (e.g., parquet files)
        generateBinaryFiles(basePath);
        //Load JSON/ADM files to HDFS
        loadData(basePath, DATA_PATH);
        //Load generated binary files (e.g., parquet files) to HDFS
        loadData(basePath, BINARY_GEN_BASEDIR);
    }

    private void loadData(File localDataRoot, String dataPath) throws IOException {
        Path destDir = new Path(HDFS_PATH);
        dfs.mkdirs(destDir);
        File srcDir = new File(localDataRoot, dataPath);
        if (srcDir.exists()) {
            File[] listOfFiles = srcDir.listFiles();
            for (File srcFile : listOfFiles) {
                if (srcFile.isFile()) {
                    Path path = new Path(srcFile.getAbsolutePath());
                    dfs.copyFromLocalFile(path, destDir);
                }
            }
        }
    }

    private void cleanupLocal() throws IOException {
        // cleanup artifacts created on the local file system
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
    }

    private void generateBinaryFiles(File localDataRoot) throws IOException {
        File srcPath = new File(localDataRoot, DATA_PATH);
        File destPath = new File(localDataRoot, BINARY_GEN_BASEDIR);
        //Delete old generated files
        IoUtil.delete(destPath);
        Files.createDirectory(Paths.get(destPath.getAbsolutePath()));
        //Write parquet files
        writeParquetDir(new File(srcPath, "parquet"), destPath);
    }

    public void cleanup() throws Exception {
        if (dfsCluster != null) {
            dfsCluster.shutdown();
            cleanupLocal();
        }
    }

    public static void main(String[] args) throws Exception {
        HDFSCluster cluster = new HDFSCluster();
        cluster.setup();
        JobConf conf = configureJobConf();
        InputSplit[] inputSplits = conf.getInputFormat().getSplits(conf, 0);
        for (InputSplit split : inputSplits) {
            System.out.println("split :" + split);
        }
    }

    private static JobConf configureJobConf() throws Exception {
        JobConf conf = new JobConf();
        String hdfsUrl = "hdfs://127.0.0.1:31888";
        String hdfsPath = "/asterix/extrasmalltweets.txt";
        conf.set("fs.default.name", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.setClassLoader(GenericAdapter.class.getClassLoader());
        conf.set("mapred.input.dir", hdfsPath);
        conf.set("mapred.input.format.class", TextInputFormat.class.getName());
        return conf;
    }

    private void writeParquetDir(File parquetSrcDir, File destPath) throws IOException {
        File[] listOfFiles = parquetSrcDir.listFiles();
        for (File jsonFile : listOfFiles) {
            String fileName = jsonFile.getName().substring(0, jsonFile.getName().indexOf(".")) + ".parquet";
            Path outputPath = new Path(destPath.getAbsolutePath(), fileName);
            writeParquetFile(jsonFile, outputPath);
        }
    }

    public void writeParquetFile(File jsonInputPath, Path parquetOutputPath) throws IOException {
        final FileInputStream schemaInputStream = new FileInputStream(jsonInputPath);
        final FileInputStream jsonInputStream = new FileInputStream(jsonInputPath);
        //Infer Avro schema
        final Schema inputSchema = JsonUtil.inferSchema(schemaInputStream, "parquet_schema", NUM_OF_RECORDS_SCHEMA);
        try (JSONFileReader<Record> reader = new JSONFileReader<>(jsonInputStream, inputSchema, Record.class)) {
            reader.initialize();
            try (AvroParquetWriter<Record> writer = new AvroParquetWriter<>(parquetOutputPath, inputSchema)) {
                for (Record record : reader) {
                    writer.write(record);
                }
            }
        }
    }
}
