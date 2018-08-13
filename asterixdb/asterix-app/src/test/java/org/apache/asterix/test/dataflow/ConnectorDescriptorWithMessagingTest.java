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
package org.apache.asterix.test.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.test.common.TestTupleGenerator;
import org.apache.asterix.test.common.TestTupleGenerator.FieldType;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.api.test.TestFrameWriter;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.MessagingFrameTupleAppender;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningWithMessageConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.PartitionWithMessageDataWriter;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.util.trace.ITraceCategoryRegistry;
import org.apache.hyracks.util.trace.ITracer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ConnectorDescriptorWithMessagingTest {

    // Tuples used in this test are made of Fields of {Integer,Double,Boolean,UTF8String};
    private static final int NUMBER_OF_CONSUMERS = 5;
    private static final int DEFAULT_FRAME_SIZE = 32768;
    private static final int CURRENT_PRODUCER = 0;
    private static final int STRING_FIELD_SIZES = 32;

    @Test
    public void testEmptyFrames() throws Exception {
        try {
            List<Integer> routing = Arrays.asList(0, 1, 2, 3, 4);
            IConnectorDescriptorRegistry connDescRegistry = Mockito.mock(IConnectorDescriptorRegistry.class);
            ITuplePartitionComputerFactory partitionComputerFactory = new TestPartitionComputerFactory(routing);
            MToNPartitioningWithMessageConnectorDescriptor connector =
                    new MToNPartitioningWithMessageConnectorDescriptor(connDescRegistry, partitionComputerFactory);
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            VSizeFrame message = new VSizeFrame(ctx);
            VSizeFrame tempBuffer = new VSizeFrame(ctx);
            TaskUtil.put(HyracksConstants.KEY_MESSAGE, message, ctx);
            message.getBuffer().clear();
            message.getBuffer().put(MessagingFrameTupleAppender.NULL_FEED_MESSAGE);
            message.getBuffer().flip();
            ISerializerDeserializer<?>[] serdes = new ISerializerDeserializer<?>[] {
                    Integer64SerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    BooleanSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };
            RecordDescriptor rDesc = new RecordDescriptor(serdes);
            TestPartitionWriterFactory partitionWriterFactory = new TestPartitionWriterFactory();
            PartitionWithMessageDataWriter partitioner =
                    (PartitionWithMessageDataWriter) connector.createPartitioner(ctx, rDesc, partitionWriterFactory,
                            CURRENT_PRODUCER, NUMBER_OF_CONSUMERS, NUMBER_OF_CONSUMERS);
            List<TestFrameWriter> recipients = new ArrayList<>();
            try {
                partitioner.open();
                FrameTupleAccessor fta = new FrameTupleAccessor(rDesc);
                for (IFrameWriter writer : partitionWriterFactory.getWriters().values()) {
                    recipients.add((TestFrameWriter) writer);
                }
                partitioner.flush(ITracer.NONE, null, ITraceCategoryRegistry.CATEGORIES_NONE, null);
                for (TestFrameWriter writer : recipients) {
                    Assert.assertEquals(writer.nextFrameCount(), 1);
                    fta.reset(writer.getLastFrame());
                    Assert.assertEquals(fta.getTupleCount(), 1);
                    FeedUtils.processFeedMessage(writer.getLastFrame(), tempBuffer, fta);
                    Assert.assertEquals(MessagingFrameTupleAppender.NULL_FEED_MESSAGE,
                            MessagingFrameTupleAppender.getMessageType(tempBuffer));
                }
                message.getBuffer().clear();
                message.getBuffer().put(MessagingFrameTupleAppender.ACK_REQ_FEED_MESSAGE);
                message.getBuffer().flip();
                partitioner.flush(ITracer.NONE, null, ITraceCategoryRegistry.CATEGORIES_NONE, null);
                for (TestFrameWriter writer : recipients) {
                    Assert.assertEquals(writer.nextFrameCount(), 2);
                    fta.reset(writer.getLastFrame());
                    Assert.assertEquals(fta.getTupleCount(), 1);
                    FeedUtils.processFeedMessage(writer.getLastFrame(), tempBuffer, fta);
                    Assert.assertEquals(MessagingFrameTupleAppender.ACK_REQ_FEED_MESSAGE,
                            MessagingFrameTupleAppender.getMessageType(tempBuffer));
                }

                message.getBuffer().clear();
                message.getBuffer().put(MessagingFrameTupleAppender.NULL_FEED_MESSAGE);
                message.getBuffer().flip();
                partitioner.flush(ITracer.NONE, null, ITraceCategoryRegistry.CATEGORIES_NONE, null);
                for (TestFrameWriter writer : recipients) {
                    Assert.assertEquals(writer.nextFrameCount(), 3);
                    fta.reset(writer.getLastFrame());
                    Assert.assertEquals(fta.getTupleCount(), 1);
                    FeedUtils.processFeedMessage(writer.getLastFrame(), tempBuffer, fta);
                    Assert.assertEquals(MessagingFrameTupleAppender.NULL_FEED_MESSAGE,
                            MessagingFrameTupleAppender.getMessageType(tempBuffer));
                }
            } catch (Throwable t) {
                partitioner.fail();
                throw t;
            } finally {
                partitioner.close();
            }
            for (TestFrameWriter writer : recipients) {
                Assert.assertEquals(writer.nextFrameCount(), 4);
                Assert.assertEquals(writer.closeCount(), 1);
            }

        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    @Test
    public void testMessageLargerThanEmptyFrame() throws Exception {
        try {
            List<Integer> routing = Arrays.asList(0, 1, 2, 3, 4);
            IConnectorDescriptorRegistry connDescRegistry = Mockito.mock(IConnectorDescriptorRegistry.class);
            ITuplePartitionComputerFactory partitionComputerFactory = new TestPartitionComputerFactory(routing);
            MToNPartitioningWithMessageConnectorDescriptor connector =
                    new MToNPartitioningWithMessageConnectorDescriptor(connDescRegistry, partitionComputerFactory);
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            VSizeFrame message = new VSizeFrame(ctx);
            VSizeFrame tempBuffer = new VSizeFrame(ctx);
            TaskUtil.put(HyracksConstants.KEY_MESSAGE, message, ctx);
            writeRandomMessage(message, MessagingFrameTupleAppender.MARKER_MESSAGE, DEFAULT_FRAME_SIZE + 1);
            ISerializerDeserializer<?>[] serdes = new ISerializerDeserializer<?>[] {
                    Integer64SerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    BooleanSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };
            RecordDescriptor rDesc = new RecordDescriptor(serdes);
            TestPartitionWriterFactory partitionWriterFactory = new TestPartitionWriterFactory();
            PartitionWithMessageDataWriter partitioner =
                    (PartitionWithMessageDataWriter) connector.createPartitioner(ctx, rDesc, partitionWriterFactory,
                            CURRENT_PRODUCER, NUMBER_OF_CONSUMERS, NUMBER_OF_CONSUMERS);
            partitioner.open();
            FrameTupleAccessor fta = new FrameTupleAccessor(rDesc);
            List<TestFrameWriter> recipients = new ArrayList<>();
            for (IFrameWriter writer : partitionWriterFactory.getWriters().values()) {
                recipients.add((TestFrameWriter) writer);
            }
            partitioner.flush(ITracer.NONE, null, ITraceCategoryRegistry.CATEGORIES_NONE, null);
            for (TestFrameWriter writer : recipients) {
                Assert.assertEquals(writer.nextFrameCount(), 1);
                fta.reset(writer.getLastFrame());
                Assert.assertEquals(fta.getTupleCount(), 1);
                FeedUtils.processFeedMessage(writer.getLastFrame(), tempBuffer, fta);
                Assert.assertEquals(MessagingFrameTupleAppender.MARKER_MESSAGE,
                        MessagingFrameTupleAppender.getMessageType(tempBuffer));
            }
            message.getBuffer().clear();
            message.getBuffer().put(MessagingFrameTupleAppender.ACK_REQ_FEED_MESSAGE);
            message.getBuffer().flip();
            partitioner.flush(ITracer.NONE, null, ITraceCategoryRegistry.CATEGORIES_NONE, null);
            for (TestFrameWriter writer : recipients) {
                Assert.assertEquals(writer.nextFrameCount(), 2);
                fta.reset(writer.getLastFrame());
                Assert.assertEquals(fta.getTupleCount(), 1);
                FeedUtils.processFeedMessage(writer.getLastFrame(), tempBuffer, fta);
                Assert.assertEquals(MessagingFrameTupleAppender.ACK_REQ_FEED_MESSAGE,
                        MessagingFrameTupleAppender.getMessageType(tempBuffer));
            }
            message.getBuffer().clear();
            message.getBuffer().put(MessagingFrameTupleAppender.NULL_FEED_MESSAGE);
            message.getBuffer().flip();
            partitioner.flush(ITracer.NONE, null, ITraceCategoryRegistry.CATEGORIES_NONE, null);
            for (TestFrameWriter writer : recipients) {
                Assert.assertEquals(writer.nextFrameCount(), 3);
                fta.reset(writer.getLastFrame());
                Assert.assertEquals(fta.getTupleCount(), 1);
                FeedUtils.processFeedMessage(writer.getLastFrame(), tempBuffer, fta);
                Assert.assertEquals(MessagingFrameTupleAppender.NULL_FEED_MESSAGE,
                        MessagingFrameTupleAppender.getMessageType(tempBuffer));
            }
            partitioner.close();
            for (TestFrameWriter writer : recipients) {
                Assert.assertEquals(writer.nextFrameCount(), 4);
                Assert.assertEquals(writer.closeCount(), 1);
            }
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    private void writeRandomMessage(VSizeFrame frame, byte tag, int size) throws HyracksDataException {
        // We subtract 2, 1 for the tag, and one for the end offset
        Random random = new Random();
        byte[] bytes = new byte[size - 2];
        random.nextBytes(bytes);
        int frameSize = FrameHelper.calcAlignedFrameSizeToStore(1, size - 1, DEFAULT_FRAME_SIZE);
        frame.ensureFrameSize(frameSize);
        frame.getBuffer().clear();
        frame.getBuffer().put(tag);
        frame.getBuffer().put(bytes);
        frame.getBuffer().flip();
    }

    @Test
    public void testMessageLargerThanSome() throws Exception {
        try {
            // Routing will be to 1, 3, and 4 only. 0 and 2 will receive no tuples
            List<Integer> routing = Arrays.asList(1, 3, 4);
            IConnectorDescriptorRegistry connDescRegistry = Mockito.mock(IConnectorDescriptorRegistry.class);
            ITuplePartitionComputerFactory partitionComputerFactory = new TestPartitionComputerFactory(routing);
            MToNPartitioningWithMessageConnectorDescriptor connector =
                    new MToNPartitioningWithMessageConnectorDescriptor(connDescRegistry, partitionComputerFactory);
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            VSizeFrame message = new VSizeFrame(ctx);
            VSizeFrame tempBuffer = new VSizeFrame(ctx);
            TaskUtil.put(HyracksConstants.KEY_MESSAGE, message, ctx);
            message.getBuffer().clear();
            writeRandomMessage(message, MessagingFrameTupleAppender.MARKER_MESSAGE, DEFAULT_FRAME_SIZE);
            ISerializerDeserializer<?>[] serdes = new ISerializerDeserializer<?>[] {
                    Integer64SerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    BooleanSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };
            FieldType[] types = { FieldType.Integer64, FieldType.Double, FieldType.Boolean, FieldType.String };
            RecordDescriptor rDesc = new RecordDescriptor(serdes);
            TestPartitionWriterFactory partitionWriterFactory = new TestPartitionWriterFactory();
            PartitionWithMessageDataWriter partitioner =
                    (PartitionWithMessageDataWriter) connector.createPartitioner(ctx, rDesc, partitionWriterFactory,
                            CURRENT_PRODUCER, NUMBER_OF_CONSUMERS, NUMBER_OF_CONSUMERS);
            partitioner.open();
            FrameTupleAccessor fta = new FrameTupleAccessor(rDesc);
            List<TestFrameWriter> recipients = new ArrayList<>();
            for (int i = 0; i < partitionWriterFactory.getWriters().values().size(); i++) {
                recipients.add(partitionWriterFactory.getWriters().get(i));
            }
            TestTupleGenerator ttg = new TestTupleGenerator(types, STRING_FIELD_SIZES, true);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender appender = new FrameTupleAppender(frame);
            ITupleReference tuple = ttg.next();
            while (appender.append(tuple)) {
                tuple = ttg.next();
            }
            partitioner.nextFrame(frame.getBuffer());
            partitioner.flush(ITracer.NONE, null, ITraceCategoryRegistry.CATEGORIES_NONE, null);
            Assert.assertEquals(1, partitionWriterFactory.getWriters().get(0).nextFrameCount());
            Assert.assertEquals(2, partitionWriterFactory.getWriters().get(1).nextFrameCount());
            Assert.assertEquals(1, partitionWriterFactory.getWriters().get(2).nextFrameCount());
            Assert.assertEquals(2, partitionWriterFactory.getWriters().get(3).nextFrameCount());
            Assert.assertEquals(2, partitionWriterFactory.getWriters().get(4).nextFrameCount());
            for (TestFrameWriter writer : recipients) {
                fta.reset(writer.getLastFrame());
                Assert.assertEquals(fta.getTupleCount(), 1);
                FeedUtils.processFeedMessage(writer.getLastFrame(), tempBuffer, fta);
                Assert.assertEquals(MessagingFrameTupleAppender.MARKER_MESSAGE,
                        MessagingFrameTupleAppender.getMessageType(tempBuffer));
            }
            partitioner.close();
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    @Test
    public void testMessageFitsWithTuples() throws Exception {
        try {
            // Routing will be round robin
            List<Integer> routing = Arrays.asList(0, 1, 2, 3, 4);
            IConnectorDescriptorRegistry connDescRegistry = Mockito.mock(IConnectorDescriptorRegistry.class);
            ITuplePartitionComputerFactory partitionComputerFactory = new TestPartitionComputerFactory(routing);
            MToNPartitioningWithMessageConnectorDescriptor connector =
                    new MToNPartitioningWithMessageConnectorDescriptor(connDescRegistry, partitionComputerFactory);
            IHyracksTaskContext ctx = TestUtils.create(DEFAULT_FRAME_SIZE);
            VSizeFrame message = new VSizeFrame(ctx);
            VSizeFrame tempBuffer = new VSizeFrame(ctx);
            TaskUtil.put(HyracksConstants.KEY_MESSAGE, message, ctx);
            message.getBuffer().clear();
            message.getBuffer().put(MessagingFrameTupleAppender.ACK_REQ_FEED_MESSAGE);
            message.getBuffer().flip();
            ISerializerDeserializer<?>[] serdes = new ISerializerDeserializer<?>[] {
                    Integer64SerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    BooleanSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };
            FieldType[] types = { FieldType.Integer64, FieldType.Double, FieldType.Boolean, FieldType.String };
            RecordDescriptor rDesc = new RecordDescriptor(serdes);
            TestPartitionWriterFactory partitionWriterFactory = new TestPartitionWriterFactory();
            PartitionWithMessageDataWriter partitioner =
                    (PartitionWithMessageDataWriter) connector.createPartitioner(ctx, rDesc, partitionWriterFactory,
                            CURRENT_PRODUCER, NUMBER_OF_CONSUMERS, NUMBER_OF_CONSUMERS);
            partitioner.open();
            FrameTupleAccessor fta = new FrameTupleAccessor(rDesc);
            List<TestFrameWriter> recipients = new ArrayList<>();
            for (int i = 0; i < partitionWriterFactory.getWriters().values().size(); i++) {
                recipients.add(partitionWriterFactory.getWriters().get(i));
            }
            TestTupleGenerator ttg = new TestTupleGenerator(types, STRING_FIELD_SIZES, true);
            VSizeFrame frame = new VSizeFrame(ctx);
            FrameTupleAppender appender = new FrameTupleAppender(frame);
            for (int count = 0; count < NUMBER_OF_CONSUMERS; count++) {
                ITupleReference tuple = ttg.next();
                appender.append(tuple);
            }
            partitioner.nextFrame(frame.getBuffer());
            partitioner.flush(ITracer.NONE, null, ITraceCategoryRegistry.CATEGORIES_NONE, null);
            Assert.assertEquals(partitionWriterFactory.getWriters().get(0).nextFrameCount(), 1);
            Assert.assertEquals(partitionWriterFactory.getWriters().get(1).nextFrameCount(), 1);
            Assert.assertEquals(partitionWriterFactory.getWriters().get(2).nextFrameCount(), 1);
            Assert.assertEquals(partitionWriterFactory.getWriters().get(3).nextFrameCount(), 1);
            Assert.assertEquals(partitionWriterFactory.getWriters().get(4).nextFrameCount(), 1);
            for (TestFrameWriter writer : recipients) {
                fta.reset(writer.getLastFrame());
                Assert.assertEquals(fta.getTupleCount(), 2);
                FeedUtils.processFeedMessage(writer.getLastFrame(), tempBuffer, fta);
                Assert.assertEquals(MessagingFrameTupleAppender.ACK_REQ_FEED_MESSAGE,
                        MessagingFrameTupleAppender.getMessageType(tempBuffer));
            }
            partitioner.close();
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }
}
