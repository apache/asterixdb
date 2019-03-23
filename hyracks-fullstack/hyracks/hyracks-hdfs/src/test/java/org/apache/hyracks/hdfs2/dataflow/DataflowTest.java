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

package org.apache.hyracks.hdfs2.dataflow;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.RawBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.hdfs.MiniDFSClusterFactory;
import org.apache.hyracks.hdfs.lib.RawBinaryHashFunctionFactory;
import org.apache.hyracks.hdfs.lib.TextKeyValueParserFactory;
import org.apache.hyracks.hdfs.lib.TextTupleWriterFactory;
import org.apache.hyracks.hdfs.utils.HyracksUtils;
import org.apache.hyracks.hdfs2.scheduler.Scheduler;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.junit.Assert;

/**
 * Test the org.apache.hyracks.hdfs2.dataflow package,
 * the operators for the Hadoop new API.
 */
public class DataflowTest extends org.apache.hyracks.hdfs.dataflow.DataflowTest {

    private MiniDFSClusterFactory dfsClusterFactory = new MiniDFSClusterFactory();

    private Job conf;

    @Override
    public void setUp() throws Exception {
        conf = new Job();
        super.setUp();
    }

    @Override
    protected Configuration getConfiguration() {
        return conf.getConfiguration();
    }

    @Override
    protected MiniDFSCluster getMiniDFSCluster(Configuration conf, int numberOfNC) throws HyracksDataException {
        return dfsClusterFactory.getMiniDFSCluster(conf, numberOfNC);
    }

    /**
     * Test a job with only HDFS read and writes.
     *
     * @throws Exception
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHDFSReadWriteOperators() throws Exception {
        FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH);
        FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH));
        conf.setInputFormatClass(TextInputFormat.class);

        Scheduler scheduler = new Scheduler(HyracksUtils.CC_HOST, HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT);
        InputFormat inputFormat = ReflectionUtils.newInstance(conf.getInputFormatClass(), getConfiguration());
        List<InputSplit> splits = inputFormat.getSplits(conf);

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
}
