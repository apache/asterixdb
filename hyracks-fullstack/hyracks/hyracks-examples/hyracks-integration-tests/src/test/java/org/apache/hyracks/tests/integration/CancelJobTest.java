/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.tests.integration;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.ClusterCapacity;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.SinkOperatorDescriptor;
import org.junit.Assert;
import org.junit.Test;

public class CancelJobTest extends AbstractMultiNCIntegrationTest {

    @Test
    public void interruptJobClientAfterWaitForCompletion() throws Exception {
        // Interrupts the job client after waitForCompletion() is called.
        for (JobSpecification spec : testJobs()) {
            interruptAfterWaitForCompletion(spec);
        }
    }

    @Test
    public void cancelExecutingJobAfterWaitForCompletion() throws Exception {
        //Cancels executing jobs after waitForCompletion() is called.
        for (JobSpecification spec : testJobs()) {
            cancelAfterWaitForCompletion(spec);
        }
    }

    @Test
    public void cancelExecutingJobBeforeWaitForCompletion() throws Exception {
        //Cancels executing jobs before waitForCompletion is called.
        for (JobSpecification spec : testJobs()) {
            cancelBeforeWaitForCompletion(spec);
        }
    }

    @Test
    public void cancelExecutingJobWithoutWaitForCompletion() throws Exception {
        //Cancels executing jobs without calling waitForCompletion.
        for (JobSpecification spec : testJobs()) {
            cancelWithoutWait(spec);
        }
    }

    @Test
    public void cancelPendingJobAfterWaitForCompletion() throws Exception {
        //Cancels pending jobs after waitForCompletion() is called.
        for (JobSpecification spec : testJobs()) {
            setJobCapacity(spec);
            cancelAfterWaitForCompletion(spec);
        }
    }

    @Test
    public void cancelPendingJobBeforeWaitForCompletion() throws Exception {
        //Cancels pending jobs before waitForCompletion is called.
        for (JobSpecification spec : testJobs()) {
            setJobCapacity(spec);
            cancelBeforeWaitForCompletion(spec);
        }
    }

    @Test
    public void cancelPendingJobWithoutWaitForCompletion() throws Exception {
        //Cancels pending jobs without calling waitForCompletion.
        for (JobSpecification spec : testJobs()) {
            setJobCapacity(spec);
            cancelWithoutWait(spec);
        }
    }

    private JobSpecification[] testJobs() {
        return new JobSpecification[] { jobWithSleepSourceOp(), jobWithSleepOp() };
    }

    private void setJobCapacity(JobSpecification spec) {
        IClusterCapacity reqCapacity = new ClusterCapacity();
        reqCapacity.setAggregatedMemoryByteSize(Long.MAX_VALUE);
        spec.setRequiredClusterCapacity(reqCapacity);
    }

    private void cancelAfterWaitForCompletion(JobSpecification spec) throws Exception {
        JobId jobId = startJob(spec);
        // A thread for canceling the job.
        Thread thread = new Thread(() -> {
            try {
                synchronized (this) {
                    this.wait(500); // Make sure waitForCompletion be called first.
                }
                cancelJob(jobId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Cancels the job.
        thread.start();

        // Checks the resulting Exception.
        boolean exceptionMatched = false;
        try {
            waitForCompletion(jobId);
        } catch (Exception e) {
            exceptionMatched = true;
            Assert.assertTrue(e instanceof HyracksException);
            HyracksException hyracksException = (HyracksException) e;
            Assert.assertTrue(hyracksException.getErrorCode() == ErrorCode.JOB_CANCELED);
        } finally {
            Assert.assertTrue(exceptionMatched);
        }
        thread.join();
    }

    private void cancelBeforeWaitForCompletion(JobSpecification spec) throws Exception {
        boolean exceptionMatched = false;
        try {
            JobId jobId = startJob(spec);
            cancelJob(jobId);
            waitForCompletion(jobId);
        } catch (HyracksException e) {
            exceptionMatched = true;
            Assert.assertTrue(e.getErrorCode() == ErrorCode.JOB_CANCELED);
        } finally {
            Assert.assertTrue(exceptionMatched);
        }
    }

    private void interruptAfterWaitForCompletion(JobSpecification spec) throws Exception {
        // Submits the job
        final JobId jobIdForInterruptTest = startJob(spec);

        // Waits for completion in anther thread
        Thread thread = new Thread(() -> {
            try {
                waitForCompletion(jobIdForInterruptTest);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof InterruptedException);
            }
        });
        thread.start();

        // Interrupts the wait-for-completion thread.
        thread.interrupt();

        // Waits until the thread terminates.
        thread.join();

        // Verifies the job status.
        JobStatus jobStatus = getJobStatus(jobIdForInterruptTest);
        while (jobStatus == JobStatus.RUNNING) {
            synchronized (this) {
                // Since job cancellation is asynchronous on NCs, we have to wait there.
                wait(1000);
            }
            jobStatus = getJobStatus(jobIdForInterruptTest);
        }
        Assert.assertTrue(jobStatus == JobStatus.FAILURE);
    }

    private void cancelWithoutWait(JobSpecification spec) throws Exception {
        JobId jobId = startJob(spec);
        cancelJob(jobId);
    }

    private JobSpecification jobWithSleepSourceOp() {
        JobSpecification spec = new JobSpecification();
        SleepSourceOperatorDescriptor sourceOpDesc = new SleepSourceOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sourceOpDesc, ASTERIX_IDS);
        SinkOperatorDescriptor sinkOpDesc = new SinkOperatorDescriptor(spec, 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sinkOpDesc, ASTERIX_IDS);
        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, sourceOpDesc, 0, sinkOpDesc, 0);
        spec.addRoot(sinkOpDesc);
        return spec;
    }

    private JobSpecification jobWithSleepOp() {
        JobSpecification spec = new JobSpecification();
        FileSplit[] ordersSplits = new FileSplit[] { new ManagedFileSplit(ASTERIX_IDS[0],
                "data" + File.separator + "tpch0.001" + File.separator + "orders-part1.tbl") };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor recordDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(),
                        new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                        new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                        new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                        new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

        // File scan operator.
        FileScanOperatorDescriptor scanOp = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'),
                recordDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, scanOp, ASTERIX_IDS[0]);

        // Sleep operator.
        SleepOperatorDescriptor sleepOp = new SleepOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sleepOp, ASTERIX_IDS);

        // Sink operator.
        SinkOperatorDescriptor sinkOp = new SinkOperatorDescriptor(spec, 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sinkOp, ASTERIX_IDS);

        // Hash-repartitioning connector.
        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn1, scanOp, 0, sleepOp, 0);

        // One-to-one connector.
        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, sleepOp, 0, sinkOp, 0);
        return spec;
    }

}

class SleepSourceOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public SleepSourceOperatorDescriptor(JobSpecification spec) {
        super(spec, 0, 1);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {

            @Override
            public void initialize() throws HyracksDataException {
                try {
                    writer.open();
                    while (true) {
                        synchronized (this) {
                            wait();
                        }
                    }
                } catch (Exception e) {
                    writer.fail();
                } finally {
                    writer.close();
                }
            }
        };
    }
}

class SleepOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public SleepOperatorDescriptor(JobSpecification spec) {
        super(spec, 1, 1);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            @Override
            public void open() throws HyracksDataException {
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    while (true) {
                        synchronized (this) {
                            wait();
                        }
                    }
                } catch (Exception e) {
                    throw HyracksDataException.create(e);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                writer.close();
            }
        };
    }
}
