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
import org.junit.Assert;
import org.junit.Test;

public class ErrorReportingTest extends AbstractIntegrationTest {

    static final String EXPECTED_ERROR_MESSAGE = "expected failure";

    @Test
    public void testInitialize() throws Exception {
        JobSpecification spec = new JobSpecification();

        TestSourceOperatorDescriptor ets1 = new TestSourceOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ets1, NC1_ID);

        TestSourceOperatorDescriptor ets2 = new TestSourceOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ets2, NC1_ID);

        TestSourceOperatorDescriptor ets3 = new TestSourceOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ets3, NC1_ID);

        ExceptionRaisingOperatorDescriptor tc = new ExceptionRaisingOperatorDescriptor(spec, 3);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, tc, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), ets1, 0, tc, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), ets2, 0, tc, 1);
        spec.connect(new OneToOneConnectorDescriptor(spec), ets3, 0, tc, 2);
        spec.addRoot(tc);

        try {
            runTest(spec);
        } catch (Exception e) {
            String actualMessage = getRootCause(e).getMessage();
            Assert.assertTrue(actualMessage.equals(EXPECTED_ERROR_MESSAGE));
        }
    }

    private Throwable getRootCause(Throwable t) {
        Throwable cause = t.getCause();
        if (cause == null) {
            return t;
        }
        return getRootCause(cause);
    }
}

class TestSourceOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public TestSourceOperatorDescriptor(JobSpecification spec) {
        super(spec, 0, 1);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            private ByteBuffer frame = ctx.allocateFrame();

            @Override
            public void initialize() throws HyracksDataException {
                try {
                    writer.open();
                    writer.nextFrame(frame);
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

class ExceptionRaisingOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public ExceptionRaisingOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity) {
        super(spec, inputArity, 0);
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ExceptionRaisingActivityNode tca = new ExceptionRaisingActivityNode(new ActivityId(getOperatorId(), 0));
        builder.addActivity(this, tca);
        for (int i = 0; i < inputArity; ++i) {
            builder.addSourceEdge(i, tca, i);
        }
    }

    private class ExceptionRaisingActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public ExceptionRaisingActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            return new IOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {

                }

                @Override
                public void deinitialize() throws HyracksDataException {

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
                public IFrameWriter getInputFrameWriter(final int index) {
                    return new IFrameWriter() {
                        @Override
                        public void open() throws HyracksDataException {

                        }

                        @Override
                        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                            if (index == inputArity - 1) {
                                throw new HyracksDataException(ErrorReportingTest.EXPECTED_ERROR_MESSAGE);
                            }
                        }

                        @Override
                        public void fail() throws HyracksDataException {

                        }

                        @Override
                        public void close() throws HyracksDataException {

                        }

                        @Override
                        public void flush() throws HyracksDataException {
                        }
                    };
                }

                @Override
                public String getDisplayName() {
                    return "Exception-Raising-Activity";
                }

            };
        }
    }

}
