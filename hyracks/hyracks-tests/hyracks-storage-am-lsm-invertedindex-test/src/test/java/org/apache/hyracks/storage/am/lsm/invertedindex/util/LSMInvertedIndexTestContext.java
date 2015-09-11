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
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.util.SerdeUtils;
import org.apache.hyracks.dataflow.common.util.TupleUtils;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestContext;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.common.LSMInvertedIndexTestHarness;
import org.apache.hyracks.storage.am.lsm.invertedindex.exceptions.InvertedIndexException;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;

@SuppressWarnings("rawtypes")
public class LSMInvertedIndexTestContext extends OrderedIndexTestContext {

    public static enum InvertedIndexType {
        INMEMORY,
        ONDISK,
        LSM,
        PARTITIONED_INMEMORY,
        PARTITIONED_ONDISK,
        PARTITIONED_LSM
    };

    protected IInvertedIndex invIndex;
    protected IBinaryComparatorFactory[] allCmpFactories;
    protected IBinaryTokenizerFactory tokenizerFactory;
    protected InvertedIndexType invIndexType;
    protected InvertedIndexTokenizingTupleIterator indexTupleIter;
    protected HashSet<Comparable> allTokens = new HashSet<Comparable>();
    protected List<ITupleReference> documentCorpus = new ArrayList<ITupleReference>();

    public LSMInvertedIndexTestContext(ISerializerDeserializer[] fieldSerdes, IIndex index,
            IBinaryTokenizerFactory tokenizerFactory, InvertedIndexType invIndexType,
            InvertedIndexTokenizingTupleIterator indexTupleIter) throws HyracksDataException {
        super(fieldSerdes, index);
        invIndex = (IInvertedIndex) index;
        this.tokenizerFactory = tokenizerFactory;
        this.invIndexType = invIndexType;
        this.indexTupleIter = indexTupleIter;
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
            int[] invertedIndexFieldsForNonBulkLoadOps) throws IndexException, HyracksDataException {
        ITypeTraits[] allTypeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] allCmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes,
                fieldSerdes.length);
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
                        new VirtualFreePageManager(harness.getVirtualBufferCaches().get(0).getNumPages()),
                        invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                        new FileReference(new File(harness.getOnDiskDir())));
                break;
            }
            case PARTITIONED_INMEMORY: {
                invIndex = InvertedIndexUtils.createPartitionedInMemoryBTreeInvertedindex(harness
                        .getVirtualBufferCaches().get(0), new VirtualFreePageManager(harness.getVirtualBufferCaches()
                        .get(0).getNumPages()), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, tokenizerFactory, new FileReference(new File(harness.getOnDiskDir())));
                break;
            }
            case ONDISK: {
                invIndex = InvertedIndexUtils.createOnDiskInvertedIndex(harness.getDiskBufferCache(),
                        harness.getDiskFileMapProvider(), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, harness.getInvListsFileRef());
                break;
            }
            case PARTITIONED_ONDISK: {
                invIndex = InvertedIndexUtils.createPartitionedOnDiskInvertedIndex(harness.getDiskBufferCache(),
                        harness.getDiskFileMapProvider(), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, harness.getInvListsFileRef());
                break;
            }
            case LSM: {
                invIndex = InvertedIndexUtils.createLSMInvertedIndex(harness.getVirtualBufferCaches(),
                        harness.getDiskFileMapProvider(), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, tokenizerFactory, harness.getDiskBufferCache(), harness.getOnDiskDir(),
                        harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(),
                        harness.getOperationTracker(), harness.getIOScheduler(), harness.getIOOperationCallback(),
                        invertedIndexFields, filterTypeTraits, filterCmpFactories, filterFields,
                        filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, true);
                break;
            }
            case PARTITIONED_LSM: {
                invIndex = InvertedIndexUtils.createPartitionedLSMInvertedIndex(harness.getVirtualBufferCaches(),
                        harness.getDiskFileMapProvider(), invListTypeTraits, invListCmpFactories, tokenTypeTraits,
                        tokenCmpFactories, tokenizerFactory, harness.getDiskBufferCache(), harness.getOnDiskDir(),
                        harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(),
                        harness.getOperationTracker(), harness.getIOScheduler(), harness.getIOOperationCallback(),
                        invertedIndexFields, filterTypeTraits, filterCmpFactories, filterFields,
                        filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, true);
                break;
            }
            default: {
                throw new InvertedIndexException("Unknow inverted-index type '" + invIndexType + "'.");
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
                indexTupleIter = new PartitionedInvertedIndexTokenizingTupleIterator(
                        invIndex.getTokenTypeTraits().length, invIndex.getInvListTypeTraits().length,
                        tokenizerFactory.createTokenizer());
                break;
            }
            default: {
                throw new InvertedIndexException("Unknow inverted-index type '" + invIndexType + "'.");
            }
        }
        LSMInvertedIndexTestContext testCtx = new LSMInvertedIndexTestContext(fieldSerdes, invIndex, tokenizerFactory,
                invIndexType, indexTupleIter);
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
            ByteArrayInputStream bains = new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i));
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
