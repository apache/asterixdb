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

package org.apache.hyracks.storage.am.lsm.invertedindex.util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.common.LSMInvertedIndexTestHarness;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.LSMInvertedIndexTestUtils.HyracksTaskTestContext;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.util.trace.ITraceCategoryRegistry;
import org.apache.hyracks.util.trace.TraceCategoryRegistry;
import org.apache.hyracks.util.trace.Tracer;

@SuppressWarnings("rawtypes")
public class LSMInvertedIndexTestContext extends OrderedIndexTestContext {

    public static enum InvertedIndexType {
        INMEMORY,
        ONDISK,
        LSM,
        PARTITIONED_INMEMORY,
        PARTITIONED_ONDISK,
        PARTITIONED_LSM
    }

    protected IInvertedIndex invIndex;
    protected IBinaryComparatorFactory[] allCmpFactories;
    protected IBinaryTokenizerFactory tokenizerFactory;
    protected InvertedIndexType invIndexType;
    protected InvertedIndexTokenizingTupleIterator indexTupleIter;
    protected HashSet<Comparable> allTokens = new HashSet<>();
    protected List<ITupleReference> documentCorpus = new ArrayList<>();

    public LSMInvertedIndexTestContext(ISerializerDeserializer[] fieldSerdes, IIndex index,
            IBinaryTokenizerFactory tokenizerFactory, InvertedIndexType invIndexType,
            InvertedIndexTokenizingTupleIterator indexTupleIter) throws HyracksDataException {
        super(fieldSerdes, index, false);
        invIndex = (IInvertedIndex) index;
        this.tokenizerFactory = tokenizerFactory;
        this.invIndexType = invIndexType;
        this.indexTupleIter = indexTupleIter;
        // Dummy hyracks task context for the test purpose only
        IHyracksTaskContext ctx = new HyracksTaskTestContext();
        // Intermediate and final search result will use this buffer manager to get frames.
        IDeallocatableFramePool framePool = new DeallocatableFramePool(ctx,
                AccessMethodTestsConfig.LSM_INVINDEX_SEARCH_FRAME_LIMIT * ctx.getInitialFrameSize());
        ISimpleFrameBufferManager bufferManagerForSearch = new FramePoolBackedFrameBufferManager(framePool);
        // Keep the buffer manager in the hyracks context so that the search process can get it via the context.
        TaskUtil.put(HyracksConstants.INVERTED_INDEX_SEARCH_FRAME_MANAGER, bufferManagerForSearch, ctx);
        IIndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(HyracksConstants.HYRACKS_TASK_CONTEXT, ctx);
        indexAccessor = index.createAccessor(iap);
    }

    @Override
    public int getKeyFieldCount() {
        return fieldSerdes.length;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        if (allCmpFactories == null) {
            // Concatenate token and inv-list comparators.
            IInvertedIndex invIndex = (IInvertedIndex) index;
            IBinaryComparatorFactory[] tokenCmpFactories = invIndex.getTokenCmpFactories();
            IBinaryComparatorFactory[] invListCmpFactories = invIndex.getInvListCmpFactories();
            int totalCmpCount = tokenCmpFactories.length + invListCmpFactories.length;
            allCmpFactories = new IBinaryComparatorFactory[totalCmpCount];
            for (int i = 0; i < tokenCmpFactories.length; i++) {
                allCmpFactories[i] = tokenCmpFactories[i];
            }
            for (int i = 0; i < invListCmpFactories.length; i++) {
                allCmpFactories[i + tokenCmpFactories.length] = invListCmpFactories[i];
            }
        }
        return allCmpFactories;
    }

    public static LSMInvertedIndexTestContext create(LSMInvertedIndexTestHarness harness,
            ISerializerDeserializer[] fieldSerdes, int tokenFieldCount, IBinaryTokenizerFactory tokenizerFactory,
            InvertedIndexType invIndexType, int[] invertedIndexFields, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields, int[] filterFieldsForNonBulkLoadOps,
            int[] invertedIndexFieldsForNonBulkLoadOps) throws HyracksDataException {
        ITypeTraits[] allTypeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IOManager ioManager = harness.getIOManager();
        IBinaryComparatorFactory[] allCmpFactories =
                SerdeUtils.serdesToComparatorFactories(fieldSerdes, fieldSerdes.length);
        // Set token type traits and comparators.
        ITypeTraits[] tokenTypeTraits = new ITypeTraits[tokenFieldCount];
        IBinaryComparatorFactory[] tokenCmpFactories = new IBinaryComparatorFactory[tokenFieldCount];
        for (int i = 0; i < tokenTypeTraits.length; i++) {
            tokenTypeTraits[i] = allTypeTraits[i];
            tokenCmpFactories[i] = allCmpFactories[i];
        }
        // Set inverted-list element type traits and comparators.
        int invListFieldCount = fieldSerdes.length - tokenFieldCount;
        ITypeTraits[] invListTypeTraits = new ITypeTraits[invListFieldCount];
        IBinaryComparatorFactory[] invListCmpFactories = new IBinaryComparatorFactory[invListFieldCount];
        for (int i = 0; i < invListTypeTraits.length; i++) {
            invListTypeTraits[i] = allTypeTraits[i + tokenFieldCount];
            invListCmpFactories[i] = allCmpFactories[i + tokenFieldCount];
        }
        // Create index and test context.
        IInvertedIndex invIndex;
        assert harness.getVirtualBufferCaches().size() > 0;
        switch (invIndexType) {
            case INMEMORY: {
                invIndex = InvertedIndexUtils.createInMemoryBTreeInvertedindex(harness.getVirtualBufferCaches().get(0),
                        new VirtualFreePageManager(harness.getVirtualBufferCaches().get(0)), invListTypeTraits,
                        invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                        ioManager.resolveAbsolutePath(harness.getOnDiskDir()));
                break;
            }
            case PARTITIONED_INMEMORY: {
                invIndex = InvertedIndexUtils.createPartitionedInMemoryBTreeInvertedindex(
                        harness.getVirtualBufferCaches().get(0),
                        new VirtualFreePageManager(harness.getVirtualBufferCaches().get(0)), invListTypeTraits,
                        invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                        ioManager.resolveAbsolutePath(harness.getOnDiskDir()));
                break;
            }
            case ONDISK: {
                invIndex = InvertedIndexUtils.createOnDiskInvertedIndex(ioManager, harness.getDiskBufferCache(),
                        invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories,
                        harness.getInvListsFileRef(), harness.getMetadataPageManagerFactory());
                break;
            }
            case PARTITIONED_ONDISK: {
                invIndex = InvertedIndexUtils.createPartitionedOnDiskInvertedIndex(ioManager,
                        harness.getDiskBufferCache(), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, harness.getInvListsFileRef(), harness.getMetadataPageManagerFactory());
                break;
            }
            case LSM: {
                invIndex = InvertedIndexUtils.createLSMInvertedIndex(ioManager, harness.getVirtualBufferCaches(),
                        invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                        harness.getDiskBufferCache(), harness.getOnDiskDir(), harness.getBoomFilterFalsePositiveRate(),
                        harness.getMergePolicy(), harness.getOperationTracker(), harness.getIOScheduler(),
                        harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                        invertedIndexFields, filterTypeTraits, filterCmpFactories, filterFields,
                        filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, true,
                        harness.getMetadataPageManagerFactory(),
                        new Tracer(LSMInvertedIndexTestContext.class.getSimpleName(),
                                ITraceCategoryRegistry.CATEGORIES_ALL, new TraceCategoryRegistry()));
                break;
            }
            case PARTITIONED_LSM: {
                invIndex = InvertedIndexUtils.createPartitionedLSMInvertedIndex(ioManager,
                        harness.getVirtualBufferCaches(), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, tokenizerFactory, harness.getDiskBufferCache(), harness.getOnDiskDir(),
                        harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(),
                        harness.getOperationTracker(), harness.getIOScheduler(),
                        harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                        invertedIndexFields, filterTypeTraits, filterCmpFactories, filterFields,
                        filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, true,
                        harness.getMetadataPageManagerFactory(),
                        new Tracer(LSMInvertedIndexTestContext.class.getSimpleName(),
                                ITraceCategoryRegistry.CATEGORIES_ALL, new TraceCategoryRegistry()));
                break;
            }
            default: {
                throw HyracksDataException.create(ErrorCode.UNKNOWN_INVERTED_INDEX_TYPE, invIndexType);
            }
        }
        InvertedIndexTokenizingTupleIterator indexTupleIter = null;
        switch (invIndexType) {
            case INMEMORY:
            case ONDISK:
            case LSM: {
                indexTupleIter = new InvertedIndexTokenizingTupleIterator(invIndex.getTokenTypeTraits().length,
                        invIndex.getInvListTypeTraits().length, tokenizerFactory.createTokenizer());
                break;
            }
            case PARTITIONED_INMEMORY:
            case PARTITIONED_ONDISK:
            case PARTITIONED_LSM: {
                indexTupleIter =
                        new PartitionedInvertedIndexTokenizingTupleIterator(invIndex.getTokenTypeTraits().length,
                                invIndex.getInvListTypeTraits().length, tokenizerFactory.createTokenizer());
                break;
            }
            default: {
                throw HyracksDataException.create(ErrorCode.UNKNOWN_INVERTED_INDEX_TYPE, invIndexType);
            }
        }
        LSMInvertedIndexTestContext testCtx =
                new LSMInvertedIndexTestContext(fieldSerdes, invIndex, tokenizerFactory, invIndexType, indexTupleIter);
        return testCtx;
    }

    public void insertCheckTuples(ITupleReference tuple, Collection<CheckTuple> checkTuples)
            throws HyracksDataException {
        documentCorpus.add(TupleUtils.copyTuple(tuple));
        indexTupleIter.reset(tuple);
        while (indexTupleIter.hasNext()) {
            indexTupleIter.next();
            ITupleReference insertTuple = indexTupleIter.getTuple();
            CheckTuple checkTuple = createCheckTuple(insertTuple);
            insertCheckTuple(checkTuple, checkTuples);
            allTokens.add(checkTuple.getField(0));
        }
    }

    public void deleteCheckTuples(ITupleReference tuple, Collection<CheckTuple> checkTuples)
            throws HyracksDataException {
        indexTupleIter.reset(tuple);
        while (indexTupleIter.hasNext()) {
            indexTupleIter.next();
            ITupleReference insertTuple = indexTupleIter.getTuple();
            CheckTuple checkTuple = createCheckTuple(insertTuple);
            deleteCheckTuple(checkTuple, checkTuples);
        }
    }

    public HashSet<Comparable> getAllTokens() {
        return allTokens;
    }

    @SuppressWarnings("unchecked")
    public CheckTuple createCheckTuple(ITupleReference tuple) throws HyracksDataException {
        CheckTuple checkTuple = new CheckTuple(fieldSerdes.length, fieldSerdes.length);
        for (int i = 0; i < fieldSerdes.length; i++) {
            ByteArrayInputStream bains =
                    new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            DataInput in = new DataInputStream(bains);
            Comparable field = (Comparable) fieldSerdes[i].deserialize(in);
            checkTuple.appendField(field);
        }
        return checkTuple;
    }

    @Override
    public void upsertCheckTuple(CheckTuple checkTuple, Collection<CheckTuple> checkTuples) {
        throw new UnsupportedOperationException("Upsert not supported by inverted index.");
    }

    public IBinaryTokenizerFactory getTokenizerFactory() {
        return tokenizerFactory;
    }

    public List<ITupleReference> getDocumentCorpus() {
        return documentCorpus;
    }

    public InvertedIndexType getInvertedIndexType() {
        return invIndexType;
    }
}
