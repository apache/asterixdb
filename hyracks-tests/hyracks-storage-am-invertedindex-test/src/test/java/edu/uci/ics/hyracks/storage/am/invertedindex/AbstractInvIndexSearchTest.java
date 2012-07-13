/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.invertedindex;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.FixedSizeElementInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.ITokenFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestUtils;

public abstract class AbstractInvIndexSearchTest extends AbstractInvIndexTest {
    protected final int PAGE_SIZE = 32768;
    protected final int NUM_PAGES = 100;
    protected final int MAX_OPEN_FILES = 10;
    protected final int HYRACKS_FRAME_SIZE = 32768;
    protected IHyracksTaskContext taskCtx = TestUtils.create(HYRACKS_FRAME_SIZE);

    protected IBufferCache bufferCache;
    protected IFileMapProvider fmp;

    protected FileReference invListsFile = new FileReference(new File(invListsFileName));

    protected ITypeTraits[] tokenTypeTraits = new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS };
    protected IBinaryComparatorFactory[] tokenCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(UTF8StringPointable.FACTORY) };
    protected ITypeTraits[] invListTypeTraits = new ITypeTraits[] { IntegerPointable.TYPE_TRAITS };
    protected IBinaryComparatorFactory[] invListCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(IntegerPointable.FACTORY) };

    protected InvertedIndex invIndex;

    protected Random rnd = new Random();

    protected ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
    protected ArrayTupleReference tuple = new ArrayTupleReference();

    protected ISerializerDeserializer[] insertSerde = { UTF8StringSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE };
    protected RecordDescriptor insertRecDesc = new RecordDescriptor(insertSerde);

    protected ArrayList<ArrayList<Integer>> checkInvLists = new ArrayList<ArrayList<Integer>>();

    protected int maxId = 1000000;
    protected int[] scanCountArray = new int[maxId];
    protected ArrayList<Integer> expectedResults = new ArrayList<Integer>();

    protected ISerializerDeserializer[] querySerde = { UTF8StringSerializerDeserializer.INSTANCE };
    protected RecordDescriptor queryRecDesc = new RecordDescriptor(querySerde);

    protected ArrayTupleBuilder queryTb = new ArrayTupleBuilder(querySerde.length);
    protected ArrayTupleReference queryTuple = new ArrayTupleReference();

    protected ITokenFactory tokenFactory;
    protected IBinaryTokenizer tokenizer;

    protected IIndexCursor resultCursor;

    protected abstract void setTokenizer();

    /**
     * Initialize members, generate data, and bulk load the inverted index.
     */
    @Before
    public void start() throws Exception {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, NUM_PAGES, MAX_OPEN_FILES);
        bufferCache = TestStorageManagerComponentHolder.getBufferCache(taskCtx);
        fmp = TestStorageManagerComponentHolder.getFileMapProvider(taskCtx);

        setTokenizer();

        IInvertedListBuilder invListBuilder = new FixedSizeElementInvertedListBuilder(invListTypeTraits);
        invIndex = new InvertedIndex(bufferCache, fmp, invListBuilder, invListTypeTraits, invListCmpFactories,
                tokenTypeTraits, tokenCmpFactories, invListsFile);
        invIndex.create();
        invIndex.activate();

        rnd.setSeed(50);
    }

    @After
    public void deinit() throws HyracksDataException {
        invIndex.deactivate();
        invIndex.destroy();
        bufferCache.close();
        AbstractInvIndexTest.tearDown();
    }
}
