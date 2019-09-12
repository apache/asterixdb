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
package org.apache.hyracks.storage.am.btree.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.test.CountAndThrowError;
import org.apache.hyracks.api.test.CountAndThrowException;
import org.apache.hyracks.api.test.CountAnswer;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({ BTreeUtils.class, FrameTupleAccessor.class, ArrayTupleBuilder.class,
        IndexSearchOperatorNodePushable.class, FrameUtils.class, FrameTupleAppender.class })
public class FramewriterTest {

    private CountAnswer openException = new CountAndThrowException("Exception in open()");
    private CountAnswer nextFrameException = new CountAndThrowException("Exception in nextFrame()");
    private CountAnswer failException = new CountAndThrowException("Exception in fail()");
    private CountAnswer closeException = new CountAndThrowException("Exception in close()");
    private CountAnswer openError = new CountAndThrowError("Exception in open()");
    private CountAnswer nextFrameError = new CountAndThrowError("Exception in nextFrame()");
    private CountAnswer failError = new CountAndThrowError("Exception in fail()");
    private CountAnswer closeError = new CountAndThrowError("Exception in close()");
    private CountAnswer openNormal = new CountAnswer();
    private CountAnswer nextFrameNormal = new CountAnswer();
    private CountAnswer failNormal = new CountAnswer();
    private CountAnswer closeNormal = new CountAnswer();

    private int successes = 0;
    private int failures = 0;
    private static final int BUFFER_SIZE = 32000;
    private static final int RECORDS_PER_FRAME = 3;
    public static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(BUFFER_SIZE);
    private static final int NUMBER_OF_APPENDERS = 2;
    public int counter = 0;

    public boolean validate(boolean finished) {
        // get number of open calls
        int openCount = openException.getCallCount() + openNormal.getCallCount() + openError.getCallCount();
        int nextFrameCount =
                nextFrameException.getCallCount() + nextFrameNormal.getCallCount() + nextFrameError.getCallCount();
        int failCount = failException.getCallCount() + failNormal.getCallCount() + failError.getCallCount();
        int closeCount = closeException.getCallCount() + closeNormal.getCallCount() + closeError.getCallCount();

        if (failCount > 1 || closeCount > 1 || openCount > 1) {
            failures++;
            return false;
        }
        if (openCount == 0 && (nextFrameCount > 0 || failCount > 0 || closeCount > 0)) {
            failures++;
            return false;
        }
        if (finished) {
            if (closeCount == 0 && (nextFrameCount > 0 || failCount > 0 || openCount > 0)) {
                failures++;
                return false;
            }
        }
        return true;
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    public MultiComparator mockMultiComparator() {
        MultiComparator mc = Mockito.mock(MultiComparator.class);
        return mc;
    }

    @Before
    public void setUp() throws Exception {
        // Mock static methods
        PowerMockito.mockStatic(BTreeUtils.class);
        PowerMockito.when(BTreeUtils.getSearchMultiComparator(Matchers.any(), Matchers.any()))
                .thenReturn(mockMultiComparator());
        PowerMockito.mockStatic(FrameUtils.class);

        // Custom implementation for FrameUtils that push to next frame immediately
        PowerMockito.when(
                FrameUtils.appendToWriter(Matchers.any(IFrameWriter.class), Matchers.any(IFrameTupleAppender.class),
                        Matchers.any(IFrameTupleAccessor.class), Matchers.anyInt(), Matchers.anyInt()))
                .thenAnswer(new Answer<Integer>() {
                    @Override
                    public Integer answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        IFrameWriter writer = (IFrameWriter) args[0];
                        writer.nextFrame(EMPTY_BUFFER);
                        return BUFFER_SIZE;
                    }
                });

        // create global mock for FrameTupleAccessor, ArrayTupleBuilder
        FrameTupleAccessor frameAccessor = Mockito.mock(FrameTupleAccessor.class);
        Mockito.when(frameAccessor.getTupleCount()).thenReturn(RECORDS_PER_FRAME);

        // Global custom implementations for FrameTupleAppender
        // since we have two appenders, then we need to test each test twice
        FrameTupleAppender[] appenders = mockAppenders();

        // Mock all instances of a class <Note that you need to prepare the class calling this constructor as well>
        PowerMockito.whenNew(FrameTupleAccessor.class).withAnyArguments().thenReturn(frameAccessor);
        PowerMockito.whenNew(FrameTupleAppender.class).withAnyArguments().thenAnswer(new Answer<FrameTupleAppender>() {
            @Override
            public FrameTupleAppender answer(InvocationOnMock invocation) throws Throwable {
                counter++;
                if (counter % 2 == 1) {
                    return appenders[0];
                }
                return appenders[1];
            }
        });
    }

    public static FrameTupleAppender[] mockAppenders() throws HyracksDataException {
        FrameTupleAppender[] appenders = new FrameTupleAppender[2];
        appenders[0] = Mockito.mock(FrameTupleAppender.class);
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                IFrameWriter writer = (IFrameWriter) args[0];
                writer.nextFrame(EMPTY_BUFFER);
                return null;
            }
        }).when(appenders[0]).write(Matchers.any(IFrameWriter.class), Matchers.anyBoolean());

        appenders[1] = Mockito.mock(FrameTupleAppender.class);
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                throw new HyracksDataException("couldn't flush frame");
            }
        }).when(appenders[1]).write(Matchers.any(IFrameWriter.class), Matchers.anyBoolean());

        return appenders;
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    private void resetAllCounters() {
        openException.reset();
        nextFrameException.reset();
        failException.reset();
        closeException.reset();
        openNormal.reset();
        nextFrameNormal.reset();
        failNormal.reset();
        closeNormal.reset();
        openError.reset();
        nextFrameError.reset();
        failError.reset();
        closeError.reset();
    }

    @Test
    public void test() {
        try {
            testBTreeSearchOperatorNodePushable();
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail(th.toString());
        }
        System.out.println("Number of passed tests: " + successes);
        System.out.println("Number of failed tests: " + failures);
        Assert.assertEquals(failures, 0);
    }

    private void testBTreeSearchOperatorNodePushable() throws Exception {
        /*
         * coverage
         * in open(){
         *  writer.open() succeeds vs. throws exception vs. throws error
         *   indexHelper.open() succeeds vs. throws exception
         *    createAccessor() succeeds vs. throws exception
         * }
         * in nextFrame(){
         *  indexAccessor.search succeeds vs. throws exception
         *   writeSearchResults succeeds vs. throws exception vs. throws error
         * }
         * in fail(){
         *  writer.fail() succeeds, throws exception, or throws error
         * }
         * in close(){
         *  appender.close() succeeds, throws exception, or throws error
         * }
         */
        int i = 0;
        counter = 0;
        while (i < NUMBER_OF_APPENDERS) {
            i++;
            ByteBuffer buffer = mockByteBuffer();
            IFrameWriter[] outPutFrameWriters = createOutputWriters();
            for (IFrameWriter outputWriter : outPutFrameWriters) {
                IFrameWriter[] underTest = createWriters();
                for (IFrameWriter writer : underTest) {
                    ((AbstractUnaryOutputOperatorNodePushable) writer).setOutputFrameWriter(0, outputWriter,
                            mockRecordDescriptor());
                    testWriter(writer, buffer);
                }
            }
            counter = i;
        }
    }

    private ByteBuffer mockByteBuffer() {
        return ByteBuffer.allocate(BUFFER_SIZE);
    }

    /**
     * @return a list of writers to test. these writers can be of the same type but behave differently based on included mocks
     * @throws HyracksDataException
     * @throws IndexException
     */
    public IFrameWriter[] createWriters() throws HyracksDataException {
        ArrayList<BTreeSearchOperatorNodePushable> writers = new ArrayList<>();
        Pair<IIndexDataflowHelperFactory, ISearchOperationCallbackFactory>[] pairs = pairs();
        IRecordDescriptorProvider[] recordDescProviders = mockRecDescProviders();
        int partition = 0;
        IHyracksTaskContext[] ctxs = mockIHyracksTaskContext();
        int[] keys = { 0 };
        boolean lowKeyInclusive = true;
        boolean highKeyInclusive = true;
        for (Pair<IIndexDataflowHelperFactory, ISearchOperationCallbackFactory> pair : pairs) {
            for (IRecordDescriptorProvider recordDescProvider : recordDescProviders) {
                for (IHyracksTaskContext ctx : ctxs) {
                    BTreeSearchOperatorNodePushable writer = new BTreeSearchOperatorNodePushable(ctx, partition,
                            recordDescProvider.getInputRecordDescriptor(new ActivityId(new OperatorDescriptorId(0), 0),
                                    0),
                            keys, keys, lowKeyInclusive, highKeyInclusive, keys, keys, pair.getLeft(), false, false,
                            null, pair.getRight(), false);
                    writers.add(writer);
                }
            }
        }
        // Create the framewriter using the mocks
        return writers.toArray(new IFrameWriter[writers.size()]);
    }

    private IHyracksTaskContext[] mockIHyracksTaskContext() throws HyracksDataException {
        IHyracksTaskContext ctx = Mockito.mock(IHyracksTaskContext.class);
        IHyracksJobletContext jobletCtx = Mockito.mock(IHyracksJobletContext.class);
        INCServiceContext serviceCtx = Mockito.mock(INCServiceContext.class);
        IStatsCollector collector = Mockito.mock(IStatsCollector.class);
        Mockito.when(ctx.allocateFrame()).thenReturn(mockByteBuffer());
        Mockito.when(ctx.allocateFrame(Mockito.anyInt())).thenReturn(mockByteBuffer());
        Mockito.when(ctx.getInitialFrameSize()).thenReturn(BUFFER_SIZE);
        Mockito.when(ctx.reallocateFrame(Mockito.any(), Mockito.anyInt(), Mockito.anyBoolean()))
                .thenReturn(mockByteBuffer());
        Mockito.when(ctx.getJobletContext()).thenReturn(jobletCtx);
        Mockito.when(jobletCtx.getServiceContext()).thenReturn(serviceCtx);
        Mockito.when(ctx.getStatsCollector()).thenReturn(collector);
        return new IHyracksTaskContext[] { ctx };
    }

    private IRecordDescriptorProvider[] mockRecDescProviders() {
        RecordDescriptor rDesc = mockRecordDescriptor();
        IRecordDescriptorProvider rDescProvider = Mockito.mock(IRecordDescriptorProvider.class);
        Mockito.when(rDescProvider.getInputRecordDescriptor(Mockito.any(), Mockito.anyInt())).thenReturn(rDesc);
        Mockito.when(rDescProvider.getOutputRecordDescriptor(Mockito.any(), Mockito.anyInt())).thenReturn(rDesc);
        return new IRecordDescriptorProvider[] { rDescProvider };
    }

    @SuppressWarnings("rawtypes")
    private RecordDescriptor mockRecordDescriptor() {
        ISerializerDeserializer serde = Mockito.mock(ISerializerDeserializer.class);
        RecordDescriptor rDesc = new RecordDescriptor(new ISerializerDeserializer[] { serde });
        return rDesc;
    }

    public ITreeIndex[] mockIndexes() throws HyracksDataException {
        IIndexAccessor[] indexAccessors = mockIndexAccessors();
        ITreeIndex[] indexes = new ITreeIndex[indexAccessors.length * 2];
        int j = 0;
        for (int i = 0; i < indexAccessors.length; i++) {
            indexes[j] = Mockito.mock(ITreeIndex.class);
            Mockito.when(indexes[j].createAccessor(Mockito.any())).thenReturn(indexAccessors[i]);
            j++;
            indexes[j] = Mockito.mock(ITreeIndex.class);
            Mockito.when(indexes[j].createAccessor(Mockito.any()))
                    .thenThrow(new HyracksDataException("failed to create accessor"));
            j++;
        }
        return indexes;
    }

    private IIndexAccessor[] mockIndexAccessors() throws HyracksDataException {
        IIndexCursor[] cursors = mockIndexCursors();
        IIndexAccessor[] accessors = new IIndexAccessor[cursors.length * 2];
        int j = 0;
        for (int i = 0; i < cursors.length; i++) {
            IIndexCursor cursor = cursors[i];
            IIndexAccessor accessor = Mockito.mock(IIndexAccessor.class);
            Mockito.when(accessor.createSearchCursor(Matchers.anyBoolean())).thenReturn(cursor);
            accessors[j] = accessor;
            j++;
            accessor = Mockito.mock(IIndexAccessor.class);
            Mockito.when(accessor.createSearchCursor(Matchers.anyBoolean())).thenReturn(cursor);
            Mockito.doAnswer(new Answer<Object>() {
                private int k = 0;

                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    k++;
                    if (k % 2 == 0) {
                        throw new HyracksDataException("Couldn't search index");
                    }
                    return null;
                }
            }).when(accessor).search(Matchers.any(), Matchers.any());
            accessors[j] = accessor;
            j++;
        }

        return accessors;
    }

    private IIndexCursor[] mockIndexCursors() throws HyracksDataException {
        ITupleReference[] tuples = mockTuples();
        IIndexCursor[] cursors = new IIndexCursor[tuples.length * 2];
        int j = 0;
        for (int i = 0; i < tuples.length; i++) {
            IIndexCursor cursor = Mockito.mock(IIndexCursor.class);
            Mockito.when(cursor.hasNext()).thenReturn(true, true, false);
            Mockito.when(cursor.getTuple()).thenReturn(tuples[i]);
            cursors[j] = cursor;
            j++;
            cursor = Mockito.mock(IIndexCursor.class);
            Mockito.when(cursor.hasNext()).thenReturn(true, true, false);
            Mockito.when(cursor.getTuple()).thenReturn(tuples[i]);
            Mockito.doThrow(new HyracksDataException("Failed to close cursor")).when(cursor).destroy();
            cursors[j] = cursor;
            j++;
        }
        return cursors;
    }

    private ITupleReference[] mockTuples() {
        ITupleReference tuple = Mockito.mock(ITupleReference.class);
        return new ITupleReference[] { tuple };
    }

    public IIndexDataflowHelper[] mockIndexHelpers() throws HyracksDataException {
        ITreeIndex[] indexes = mockIndexes();
        IIndexDataflowHelper[] indexHelpers = new IIndexDataflowHelper[indexes.length * 2];
        int j = 0;
        for (int i = 0; i < indexes.length; i++) {
            // normal
            indexHelpers[j] = Mockito.mock(IIndexDataflowHelper.class);
            Mockito.when(indexHelpers[j].getIndexInstance()).thenReturn(indexes[i]);

            // throws exception when opened
            j++;
            indexHelpers[j] = Mockito.mock(IIndexDataflowHelper.class);
            Mockito.doThrow(new HyracksDataException("couldn't open index")).when(indexHelpers[j]).open();
            Mockito.when(indexHelpers[j].getIndexInstance()).thenReturn(null);

            j++;
        }
        return indexHelpers;
    }

    public IIndexDataflowHelperFactory[] mockIndexHelperFactories() throws HyracksDataException {
        IIndexDataflowHelper[] helpers = mockIndexHelpers();
        IIndexDataflowHelperFactory[] indexHelperFactories = new IIndexDataflowHelperFactory[helpers.length];
        for (int i = 0; i < helpers.length; i++) {
            indexHelperFactories[i] = Mockito.mock(IIndexDataflowHelperFactory.class);
            Mockito.when(indexHelperFactories[i].create(Mockito.any(), Mockito.anyInt())).thenReturn(helpers[i]);
        }
        return indexHelperFactories;
    }

    @SuppressWarnings("unchecked")
    public Pair<IIndexDataflowHelperFactory, ISearchOperationCallbackFactory>[] pairs() throws HyracksDataException {
        IIndexDataflowHelperFactory[] indexDataflowHelperFactories = mockIndexHelperFactories();
        ISearchOperationCallbackFactory[] searchOpCallbackFactories = mockSearchOpCallbackFactories();
        Pair<IIndexDataflowHelperFactory, ISearchOperationCallbackFactory>[] pairs =
                new Pair[indexDataflowHelperFactories.length * searchOpCallbackFactories.length];
        int k = 0;
        for (int i = 0; i < indexDataflowHelperFactories.length; i++) {
            for (int j = 0; j < searchOpCallbackFactories.length; j++) {
                Pair<IIndexDataflowHelperFactory, ISearchOperationCallbackFactory> pair =
                        Pair.of(indexDataflowHelperFactories[i], searchOpCallbackFactories[j]);
                pairs[k] = pair;
                k++;
            }
        }
        return pairs;
    }

    private ISearchOperationCallbackFactory[] mockSearchOpCallbackFactories() throws HyracksDataException {
        ISearchOperationCallback searchOpCallback = mockSearchOpCallback();
        ISearchOperationCallbackFactory searchOpCallbackFactory = Mockito.mock(ISearchOperationCallbackFactory.class);
        Mockito.when(searchOpCallbackFactory.createSearchOperationCallback(Mockito.anyLong(),
                Mockito.any(IHyracksTaskContext.class), Mockito.isNull(IOperatorNodePushable.class)))
                .thenReturn(searchOpCallback);
        return new ISearchOperationCallbackFactory[] { searchOpCallbackFactory };
    }

    private ISearchOperationCallback mockSearchOpCallback() {
        ISearchOperationCallback opCallback = Mockito.mock(ISearchOperationCallback.class);
        return opCallback;
    }

    public IFrameWriter[] createOutputWriters() throws Exception {
        CountAnswer[] opens = new CountAnswer[] { openNormal, openException, openError };
        CountAnswer[] nextFrames = new CountAnswer[] { nextFrameNormal, nextFrameException, nextFrameError };
        CountAnswer[] fails = new CountAnswer[] { failNormal, failException, failError };
        CountAnswer[] closes = new CountAnswer[] { closeNormal, closeException, closeError };
        List<IFrameWriter> outputWriters = new ArrayList<>();
        for (CountAnswer openAnswer : opens) {
            for (CountAnswer nextFrameAnswer : nextFrames) {
                for (CountAnswer failAnswer : fails) {
                    for (CountAnswer closeAnswer : closes) {
                        IFrameWriter writer = Mockito.mock(IFrameWriter.class);
                        Mockito.doAnswer(openAnswer).when(writer).open();
                        Mockito.doAnswer(nextFrameAnswer).when(writer).nextFrame(Mockito.any());
                        Mockito.doAnswer(failAnswer).when(writer).fail();
                        Mockito.doAnswer(closeAnswer).when(writer).close();
                        outputWriters.add(writer);
                    }
                }
            }
        }
        return outputWriters.toArray(new IFrameWriter[outputWriters.size()]);
    }

    public void testWriter(IFrameWriter writer, ByteBuffer buffer) throws Exception {
        resetAllCounters();
        boolean failed = !validate(false);
        if (failed) {
            return;
        }
        try {
            writer.open();
            failed = !validate(false);
            if (failed) {
                return;
            }
            for (int i = 0; i < 10; i++) {
                writer.nextFrame(buffer);
                failed = !validate(false);
                if (failed) {
                    return;
                }
            }
        } catch (Throwable th1) {
            try {
                failed = !validate(false);
                if (failed) {
                    return;
                }
                writer.fail();
                failed = !validate(false);
                if (failed) {
                    return;
                }
            } catch (Throwable th2) {
                failed = !validate(false);
                if (failed) {
                    return;
                }
            }
        } finally {
            if (!failed) {
                try {
                    failed = !validate(false);
                    if (failed) {
                        return;
                    }
                    writer.close();
                    failed = !validate(true);
                    if (failed) {
                        return;
                    }
                } catch (Throwable th3) {
                    failed = !validate(true);
                    if (failed) {
                        return;
                    }
                }
            }
        }
        successes++;
    }
}
