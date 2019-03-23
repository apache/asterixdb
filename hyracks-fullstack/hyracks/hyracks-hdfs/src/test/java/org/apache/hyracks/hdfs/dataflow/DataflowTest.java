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

package org.apache.hyracks.hdfs.dataflow;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.RawBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.hdfs.lib.RawBinaryHashFunctionFactory;
import org.apache.hyracks.hdfs.lib.TextKeyValueParserFactory;
import org.apache.hyracks.hdfs.lib.TextTupleWriterFactory;
import org.apache.hyracks.hdfs.scheduler.Scheduler;
import org.apache.hyracks.hdfs.utils.HyracksUtils;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.util.file.FileUtil;
import org.junit.Assert;

import junit.framework.TestCase;

/**
 * Test the org.apache.hyracks.hdfs.dataflow package,
 * the operators for the Hadoop old API.
 */
@SuppressWarnings({ "deprecation" })
public class DataflowTest extends TestCase {

    protected static final String ACTUAL_RESULT_DIR = FileUtil.joinPath("target", "actual");
    private static final String TEST_RESOURCES = FileUtil.joinPath("src", "test", "resources");
    protected static final String EXPECTED_RESULT_PATH = FileUtil.joinPath(TEST_RESOURCES, "expected");
    private static final String PATH_TO_HADOOP_CONF = FileUtil.joinPath(TEST_RESOURCES, "hadoop", "conf");
    protected static final String BUILD_DIR = FileUtil.joinPath("target", "build");

    private static final String DATA_PATH = FileUtil.joinPath(TEST_RESOURCES, "data", "customer.tbl");
    protected static final String HDFS_INPUT_PATH = "/customer/";
    protected static final String HDFS_OUTPUT_PATH = "/customer_result/";

    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private static final String MINIDFS_BASEDIR = FileUtil.joinPath("target", "hdfs");
    private MiniDFSCluster dfsCluster;

    private JobConf conf = new JobConf();
    private int numberOfNC = 2;

    @Override
    public void setUp() throws Exception {
        cleanupStores();
        HyracksUtils.init();
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHDFS();
    }

    private void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File(MINIDFS_BASEDIR));
        FileUtils.cleanDirectory(new File(MINIDFS_BASEDIR));
    }

    protected Configuration getConfiguration() {
        return conf;
    }

    protected MiniDFSCluster getMiniDFSCluster(Configuration conf, int numberOfNC) throws IOException {
        return new MiniDFSCluster(conf, numberOfNC, true, null);
    }

    /**
     * Start the HDFS cluster and setup the data files
     *
     * @throws IOException
     */
    protected void startHDFS() throws IOException {
        getConfiguration().addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        getConfiguration().addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        getConfiguration().addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));

        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path(BUILD_DIR), true);
        System.setProperty("hadoop.log.dir", FileUtil.joinPath("target", "logs"));
        getConfiguration().set("hdfs.minidfs.basedir", MINIDFS_BASEDIR);
        dfsCluster = getMiniDFSCluster(getConfiguration(), numberOfNC);
        FileSystem dfs = FileSystem.get(getConfiguration());
        Path src = new Path(DATA_PATH);
        Path dest = new Path(HDFS_INPUT_PATH);
        Path result = new Path(HDFS_OUTPUT_PATH);
        dfs.mkdirs(dest);
        dfs.mkdirs(result);
        dfs.copyFromLocalFile(src, dest);

        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        getConfiguration().writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }

    /**
     * Test a job with only HDFS read and writes.
     *
     * @throws Exception
     */
    public void testHDFSReadWriteOperators() throws Exception {
        FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH);
        FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH));
        conf.setInputFormat(TextInputFormat.class);

        Scheduler scheduler = new Scheduler(HyracksUtils.CC_HOST, HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT);
        InputSplit[] splits = conf.getInputFormat().getSplits(conf, numberOfNC * 4);

        String[] readSchedule = scheduler.getLocationConstraints(splits);
        JobSpecification jobSpec = new JobSpecification();
        RecordDescriptor recordDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });

        String[] locations =
                new String[] { HyracksUtils.NC1_ID, HyracksUtils.NC1_ID, HyracksUtils.NC2_ID, HyracksUtils.NC2_ID };
        HDFSReadOperatorDescriptor readOperator = new HDFSReadOperatorDescriptor(jobSpec, recordDesc, conf, splits,
                readSchedule, new TextKeyValueParserFactory());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, readOperator, locations);

        ExternalSortOperatorDescriptor sortOperator = new ExternalSortOperatorDescriptor(jobSpec, 10, new int[] { 0 },
                new IBinaryComparatorFactory[] { RawBinaryComparatorFactory.INSTANCE }, recordDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, sortOperator, locations);

        HDFSWriteOperatorDescriptor writeOperator =
                new HDFSWriteOperatorDescriptor(jobSpec, conf, new TextTupleWriterFactory());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, writeOperator, HyracksUtils.NC1_ID);

        jobSpec.connect(new OneToOneConnectorDescriptor(jobSpec), readOperator, 0, sortOperator, 0);
        jobSpec.connect(
                new MToNPartitioningMergingConnectorDescriptor(jobSpec,
                        new FieldHashPartitionComputerFactory(new int[] { 0 },
                                new IBinaryHashFunctionFactory[] { RawBinaryHashFunctionFactory.INSTANCE }),
                        new int[] { 0 }, new IBinaryComparatorFactory[] { RawBinaryComparatorFactory.INSTANCE }, null),
                sortOperator, 0, writeOperator, 0);
        jobSpec.addRoot(writeOperator);

        IHyracksClientConnection client =
                new HyracksConnection(HyracksUtils.CC_HOST, HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT);
        JobId jobId = client.startJob(jobSpec);
        client.waitForCompletion(jobId);

        Assert.assertEquals(true, checkResults());
    }

    /**
     * Check if the results are correct
     *
     * @return true if correct
     * @throws Exception
     */
    protected boolean checkResults() throws Exception {
        FileSystem dfs = FileSystem.get(getConfiguration());
        Path result = new Path(HDFS_OUTPUT_PATH);
        Path actual = new Path(ACTUAL_RESULT_DIR);
        dfs.copyToLocalFile(result, actual);

        TestUtils.compareWithResult(new File(FileUtil.joinPath(EXPECTED_RESULT_PATH, "part-0")),
                new File(FileUtil.joinPath(ACTUAL_RESULT_DIR, "customer_result", "part-0")));
        return true;
    }

    /**
     * cleanup hdfs cluster
     */
    private void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
    }

    @Override
    public void tearDown() throws Exception {
        HyracksUtils.deinit();
        cleanupHDFS();
    }

}
