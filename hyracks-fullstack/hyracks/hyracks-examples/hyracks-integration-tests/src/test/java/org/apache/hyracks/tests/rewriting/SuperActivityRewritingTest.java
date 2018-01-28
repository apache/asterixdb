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
package org.apache.hyracks.tests.rewriting;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.tests.integration.AbstractIntegrationTest;
import org.junit.Test;

public class SuperActivityRewritingTest extends AbstractIntegrationTest {

    @Test
    public void testScanUnion() throws Exception {
        JobSpecification spec = new JobSpecification();

        DummySourceOperatorDescriptor ets1 = new DummySourceOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ets1, NC1_ID);

        DummySourceOperatorDescriptor ets2 = new DummySourceOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ets2, NC1_ID);

        DummySourceOperatorDescriptor ets3 = new DummySourceOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ets3, NC1_ID);

        ThreadCountingOperatorDescriptor tc = new ThreadCountingOperatorDescriptor(spec, 3);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, tc, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), ets1, 0, tc, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), ets2, 0, tc, 1);
        spec.connect(new OneToOneConnectorDescriptor(spec), ets3, 0, tc, 2);
        spec.addRoot(tc);
        runTest(spec);
    }

}

class DummySourceOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public DummySourceOperatorDescriptor(JobSpecification spec) {
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
                } catch (Exception e) {
                    writer.fail();
                    throw e;
                } finally {
                    writer.close();
                }
            }
        };
    }

}

class ThreadCountingOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public ThreadCountingOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity) {
        super(spec, inputArity, 0);
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ThreadCountingActivityNode tca = new ThreadCountingActivityNode(new ActivityId(getOperatorId(), 0));
        builder.addActivity(this, tca);
        for (int i = 0; i < inputArity; ++i) {
            builder.addSourceEdge(i, tca, i);
        }
    }

    private class ThreadCountingActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public ThreadCountingActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            return new IOperatorNodePushable() {
                private CountDownLatch allOpenedSignal = new CountDownLatch(3);
                private Set<Long> threads = new HashSet<>();

                @Override
                public void initialize() throws HyracksDataException {

                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    if (threads.size() != inputArity) {
                        throw new HyracksDataException("The number of worker threads is not as expected");
                    }
                }

                @Override
                public int getInputArity() {
                    return inputArity;
                }

                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                        throws HyracksDataException {
                    throw new IllegalStateException();
                }

                @Override
                public IFrameWriter getInputFrameWriter(int index) {
                    return new IFrameWriter() {
                        @Override
                        public void open() throws HyracksDataException {
                            allOpenedSignal.countDown();
                            synchronized (threads) {
                                threads.add(Thread.currentThread().getId());
                            }
                        }

                        @Override
                        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {

                        }

                        @Override
                        public void fail() throws HyracksDataException {

                        }

                        @Override
                        public void close() throws HyracksDataException {
                            try {
                                allOpenedSignal.await();
                            } catch (InterruptedException e) {
                                // This test should not be interrupted
                            }
                        }

                        @Override
                        public void flush() throws HyracksDataException {
                        }
                    };
                }

                @Override
                public String getDisplayName() {
                    return "Thread-Counting-Activity";
                }

            };
        }
    }

}
