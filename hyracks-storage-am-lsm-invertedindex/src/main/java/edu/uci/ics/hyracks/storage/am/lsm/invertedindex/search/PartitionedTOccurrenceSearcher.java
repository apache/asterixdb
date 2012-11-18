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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.ConcatenatingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IObjectFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAppender;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.PartitionedOnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.PartitionedOnDiskInvertedIndex.InvertedListPartitions;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.ObjectCache;

// TODO: The search procedure is rather confusing regarding cursor positions, hasNext() calls etc.
// Needs an overhaul some time.
public class PartitionedTOccurrenceSearcher implements IInvertedIndexSearcher {

    protected final IHyracksCommonContext ctx;
    protected final FixedSizeFrameTupleAppender resultFrameTupleApp;
    protected final FixedSizeFrameTupleAccessor resultFrameTupleAcc;
    protected final FixedSizeTupleReference resultTuple;
    protected final int invListKeyLength;
    protected int currentNumResults;

    protected List<ByteBuffer> newResultBuffers = new ArrayList<ByteBuffer>();
    protected List<ByteBuffer> prevResultBuffers = new ArrayList<ByteBuffer>();
    protected List<ByteBuffer> swap = null;
    protected int maxResultBufIdx = 0;

    protected RecordDescriptor queryTokenRecDesc = new RecordDescriptor(
            new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });
    protected ArrayTupleBuilder queryTokenBuilder = new ArrayTupleBuilder(queryTokenRecDesc.getFieldCount());
    protected DataOutput queryTokenDos = queryTokenBuilder.getDataOutput();
    protected FrameTupleAppender queryTokenAppender;
    protected ByteBuffer queryTokenFrame;
    protected final FrameTupleReference searchKey = new FrameTupleReference();

    protected final IInvertedIndex invIndex;
    protected final MultiComparator invListCmp;
    protected final ITypeTraits[] invListFieldsWithCount;
    protected int occurrenceThreshold;

    protected final int cursorCacheSize = 10;
    //protected List<IInvertedListCursor> invListCursorCache = new ArrayList<IInvertedListCursor>(cursorCacheSize);
    protected List<IInvertedListCursor> invListCursors = new ArrayList<IInvertedListCursor>(cursorCacheSize);

    protected final ArrayTupleBuilder lowerBoundTupleBuilder = new ArrayTupleBuilder(1);
    protected final ArrayTupleReference lowerBoundTuple = new ArrayTupleReference();
    protected final ArrayTupleBuilder upperBoundTupleBuilder = new ArrayTupleBuilder(1);
    protected final ArrayTupleReference upperBoundTuple = new ArrayTupleReference();
    protected final ConcatenatingTupleReference partLowSearchKey = new ConcatenatingTupleReference(2);
    protected final ConcatenatingTupleReference partHighSearchKey = new ConcatenatingTupleReference(2);
    
    protected final IObjectFactory<IInvertedListCursor> invListCursorFactory;
    protected final IObjectFactory<ArrayList<IInvertedListCursor>> arrayListFactory;
    protected final ObjectCache<IInvertedListCursor> invListCursorCache;
    protected final ObjectCache<ArrayList<IInvertedListCursor>> arrayListCache;
    
    protected final InvertedListPartitions partitions;
    
    public PartitionedTOccurrenceSearcher(IHyracksCommonContext ctx, IInvertedIndex invIndex) {
        this.ctx = ctx;
        this.invIndex = invIndex;
        this.invListCmp = MultiComparator.create(invIndex.getInvListCmpFactories());

        ITypeTraits[] invListFields = invIndex.getInvListTypeTraits();
        invListFieldsWithCount = new ITypeTraits[invListFields.length + 1];
        int tmp = 0;
        for (int i = 0; i < invListFields.length; i++) {
            invListFieldsWithCount[i] = invListFields[i];
            tmp += invListFields[i].getFixedLength();
        }
        // using an integer for counting occurrences
        invListFieldsWithCount[invListFields.length] = IntegerPointable.TYPE_TRAITS;
        invListKeyLength = tmp;

        resultFrameTupleApp = new FixedSizeFrameTupleAppender(ctx.getFrameSize(), invListFieldsWithCount);
        resultFrameTupleAcc = new FixedSizeFrameTupleAccessor(ctx.getFrameSize(), invListFieldsWithCount);
        resultTuple = new FixedSizeTupleReference(invListFieldsWithCount);
        newResultBuffers.add(ctx.allocateFrame());
        prevResultBuffers.add(ctx.allocateFrame());

        queryTokenAppender = new FrameTupleAppender(ctx.getFrameSize());
        queryTokenFrame = ctx.allocateFrame();

        invListCursorFactory = new InvertedListCursorFactory(invIndex);
        arrayListFactory = new ArrayListFactory<IInvertedListCursor>();        
        invListCursorCache = new ObjectCache<IInvertedListCursor>(invListCursorFactory, 10, 10);
        arrayListCache = new ObjectCache<ArrayList<IInvertedListCursor>>(arrayListFactory, 10, 10);
        
        PartitionedOnDiskInvertedIndex partInvIndex = (PartitionedOnDiskInvertedIndex) invIndex;
        partitions = partInvIndex.new InvertedListPartitions(invListCursorCache, arrayListCache);
        
        currentNumResults = 0;
    }

    public void reset() {
        for (ByteBuffer b : newResultBuffers) {
            resultFrameTupleApp.reset(b, true);
        }
        for (ByteBuffer b : prevResultBuffers) {
            resultFrameTupleApp.reset(b, true);
        }
        currentNumResults = 0;
    }

    public void search(OnDiskInvertedIndexSearchCursor resultCursor, InvertedIndexSearchPredicate searchPred,
            IIndexOperationContext ictx) throws HyracksDataException, IndexException {
        ITupleReference queryTuple = searchPred.getQueryTuple();
        int queryFieldIndex = searchPred.getQueryFieldIndex();
        IInvertedIndexSearchModifier searchModifier = searchPred.getSearchModifier();
        IBinaryTokenizer queryTokenizer = searchPred.getQueryTokenizer();

        queryTokenAppender.reset(queryTokenFrame, true);
        queryTokenizer.reset(queryTuple.getFieldData(queryFieldIndex), queryTuple.getFieldStart(queryFieldIndex),
                queryTuple.getFieldLength(queryFieldIndex));

        while (queryTokenizer.hasNext()) {
            queryTokenizer.next();
            queryTokenBuilder.reset();
            try {
                IToken token = queryTokenizer.getToken();
                token.serializeToken(queryTokenDos);
                queryTokenBuilder.addFieldEndOffset();
                // WARNING: assuming one frame is big enough to hold all tokens
                queryTokenAppender.append(queryTokenBuilder.getFieldEndOffsets(), queryTokenBuilder.getByteArray(), 0,
                        queryTokenBuilder.getSize());
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        FrameTupleAccessor queryTokenAccessor = new FrameTupleAccessor(ctx.getFrameSize(), queryTokenRecDesc);
        queryTokenAccessor.reset(queryTokenFrame);
        int numQueryTokens = queryTokenAccessor.getTupleCount();
        
        // ALEX NEW CODE STARTS HERE
        int numTokensLowerBound = searchModifier.getNumTokensLowerBound(numQueryTokens);
        int numTokensUpperBound = searchModifier.getNumTokensLowerBound(numQueryTokens);
        ITupleReference lowSearchKey = null;
        ITupleReference highSearchKey = null;
        try {
            if (numTokensLowerBound >= 0) {
                lowerBoundTupleBuilder.reset();
                lowerBoundTupleBuilder.getDataOutput().writeInt(numTokensLowerBound);
                lowerBoundTupleBuilder.addFieldEndOffset();
                lowerBoundTuple.reset(lowerBoundTupleBuilder.getFieldEndOffsets(),
                        lowerBoundTupleBuilder.getByteArray());
                partLowSearchKey.reset();
                partLowSearchKey.addTuple(searchKey);
                partLowSearchKey.addTuple(lowerBoundTuple);
                lowSearchKey = partLowSearchKey;
            } else {
                lowSearchKey = searchKey;
            }
            if (numTokensUpperBound >= 0) {
                upperBoundTupleBuilder.reset();
                upperBoundTupleBuilder.getDataOutput().writeInt(numTokensUpperBound);
                upperBoundTupleBuilder.addFieldEndOffset();
                upperBoundTuple.reset(upperBoundTupleBuilder.getFieldEndOffsets(),
                        upperBoundTupleBuilder.getByteArray());
                partHighSearchKey.reset();
                partHighSearchKey.addTuple(searchKey);
                partHighSearchKey.addTuple(upperBoundTuple);
                highSearchKey = partHighSearchKey;
            } else {
                highSearchKey = searchKey;
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        occurrenceThreshold = searchModifier.getOccurrenceThreshold(numQueryTokens);
        // TODO: deal with panic cases properly
        if (occurrenceThreshold <= 0) {
            throw new OccurrenceThresholdPanicException("Merge Threshold is <= 0. Failing Search.");
        }

        PartitionedOnDiskInvertedIndex partInvIndex = (PartitionedOnDiskInvertedIndex) invIndex;
        partitions.reset(numTokensLowerBound, numTokensUpperBound);
        for (int i = 0; i < numQueryTokens; i++) {
            searchKey.reset(queryTokenAccessor, i);
            partInvIndex.openInvertedListPartitionCursors(partitions, lowSearchKey, highSearchKey, ictx);
        }

        // Process the partitions one-by-one.
        ArrayList<IInvertedListCursor>[] partitionCursors = partitions.getPartitions();
        int start = (numTokensLowerBound >= 0) ? numTokensLowerBound : 0;
        int end = (numTokensUpperBound >= 0) ? numTokensUpperBound : partitionCursors.length - 1;
        for (int i = start; i <= end; i++) {
            if (partitionCursors[i] == null) {
                continue;
            }
            // Prune partition because no element in it can satisfy the occurrence threshold.
            if (partitionCursors[i].size() < occurrenceThreshold) {
                continue;
            }
            
            // Process partition.
            Collections.sort(partitionCursors[i]);
            // TODO: Continue here.
            
        }
        
        
        
        
        int numPrefixLists = searchModifier.getNumPrefixLists(invListCursors.size());
        maxResultBufIdx = mergePrefixLists(numPrefixLists, numQueryTokens);
        maxResultBufIdx = mergeSuffixLists(numPrefixLists, numQueryTokens, maxResultBufIdx);

        resultCursor.open(null, searchPred);
    }

    protected int mergePrefixLists(int numPrefixTokens, int numQueryTokens) throws HyracksDataException, IndexException {
        int maxPrevBufIdx = 0;
        for (int i = 0; i < numPrefixTokens; i++) {
            swap = prevResultBuffers;
            prevResultBuffers = newResultBuffers;
            newResultBuffers = swap;
            currentNumResults = 0;

            invListCursors.get(i).pinPages();
            maxPrevBufIdx = mergePrefixList(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx, newResultBuffers);
            invListCursors.get(i).unpinPages();
        }
        return maxPrevBufIdx;
    }

    protected int mergeSuffixLists(int numPrefixTokens, int numQueryTokens, int maxPrevBufIdx)
            throws HyracksDataException, IndexException {
        for (int i = numPrefixTokens; i < numQueryTokens; i++) {
            swap = prevResultBuffers;
            prevResultBuffers = newResultBuffers;
            newResultBuffers = swap;

            invListCursors.get(i).pinPages();
            int numInvListElements = invListCursors.get(i).size();
            // should we binary search the next list or should we sort-merge it?
            if (currentNumResults * Math.log(numInvListElements) < currentNumResults + numInvListElements) {
                maxPrevBufIdx = mergeSuffixListProbe(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx,
                        newResultBuffers, i, numQueryTokens);
            } else {
                maxPrevBufIdx = mergeSuffixListScan(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx,
                        newResultBuffers, i, numQueryTokens);
            }
            invListCursors.get(i).unpinPages();
        }
        return maxPrevBufIdx;
    }

    protected int mergeSuffixListProbe(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers,
            int maxPrevBufIdx, List<ByteBuffer> newResultBuffers, int invListIx, int numQueryTokens) throws HyracksDataException, IndexException {

        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);

        int resultTidx = 0;

        currentNumResults = 0;

        resultFrameTupleAcc.reset(prevCurrentBuffer);
        resultFrameTupleApp.reset(newCurrentBuffer, true);

        while (resultTidx < resultFrameTupleAcc.getTupleCount()) {

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));
            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                    resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));

            if (invListCursor.containsKey(resultTuple, invListCmp)) {
                count++;
                newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
            } else {
                if (count + numQueryTokens - invListIx > occurrenceThreshold) {
                    newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
                }
            }

            resultTidx++;
            if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                prevBufIdx++;
                if (prevBufIdx <= maxPrevBufIdx) {
                    prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                    resultFrameTupleAcc.reset(prevCurrentBuffer);
                    resultTidx = 0;
                }
            }
        }

        return newBufIdx;
    }

    protected int mergeSuffixListScan(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers,
            int maxPrevBufIdx, List<ByteBuffer> newResultBuffers, int invListIx, int numQueryTokens)
            throws HyracksDataException, IndexException {
        
        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);

        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0;

        currentNumResults = 0;
        
        resultFrameTupleAcc.reset(prevCurrentBuffer);
        resultFrameTupleApp.reset(newCurrentBuffer, true);

        int invListTidx = 0;
        int invListNumTuples = invListCursor.size();

        if (invListCursor.hasNext())
            invListCursor.next();

        while (invListTidx < invListNumTuples && resultTidx < resultFrameTupleAcc.getTupleCount()) {

            ITupleReference invListTuple = invListCursor.getTuple();

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int cmp = invListCmp.compare(invListTuple, resultTuple);
            if (cmp == 0) {
                int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                        resultTuple.getFieldStart(resultTuple.getFieldCount() - 1)) + 1;
                newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
                advanceCursor = true;
                advancePrevResult = true;
            } else {
                if (cmp < 0) {
                    advanceCursor = true;
                    advancePrevResult = false;
                } else {
                    int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                            resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
                    if (count + numQueryTokens - invListIx > occurrenceThreshold) {
                        newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
                    }
                    advanceCursor = false;
                    advancePrevResult = true;
                }
            }

            if (advancePrevResult) {
                resultTidx++;
                if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                    prevBufIdx++;
                    if (prevBufIdx <= maxPrevBufIdx) {
                        prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                        resultFrameTupleAcc.reset(prevCurrentBuffer);
                        resultTidx = 0;
                    }
                }
            }

            if (advanceCursor) {
                invListTidx++;
                if (invListCursor.hasNext()) {
                    invListCursor.next();
                }
            }
        }

        // append remaining elements from previous result set
        while (resultTidx < resultFrameTupleAcc.getTupleCount()) {

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                    resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
            if (count + numQueryTokens - invListIx > occurrenceThreshold) {
                newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
            }

            resultTidx++;
            if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                prevBufIdx++;
                if (prevBufIdx <= maxPrevBufIdx) {
                    prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                    resultFrameTupleAcc.reset(prevCurrentBuffer);
                    resultTidx = 0;
                }
            }
        }

        return newBufIdx;
    }

    protected int mergePrefixList(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers,
            int maxPrevBufIdx, List<ByteBuffer> newResultBuffers) throws HyracksDataException, IndexException {
        
        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);

        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0;

        resultFrameTupleAcc.reset(prevCurrentBuffer);
        resultFrameTupleApp.reset(newCurrentBuffer, true);

        int invListTidx = 0;
        int invListNumTuples = invListCursor.size();

        if (invListCursor.hasNext())
            invListCursor.next();

        while (invListTidx < invListNumTuples && resultTidx < resultFrameTupleAcc.getTupleCount()) {

            ITupleReference invListTuple = invListCursor.getTuple();
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int cmp = invListCmp.compare(invListTuple, resultTuple);
            if (cmp == 0) {
                int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                        resultTuple.getFieldStart(resultTuple.getFieldCount() - 1)) + 1;
                newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
                advanceCursor = true;
                advancePrevResult = true;
            } else {
                if (cmp < 0) {
                    int count = 1;
                    newBufIdx = appendTupleToNewResults(invListTuple, count, newBufIdx);
                    advanceCursor = true;
                    advancePrevResult = false;
                } else {
                    int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                            resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
                    newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
                    advanceCursor = false;
                    advancePrevResult = true;
                }
            }

            if (advancePrevResult) {
                resultTidx++;
                if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                    prevBufIdx++;
                    if (prevBufIdx <= maxPrevBufIdx) {
                        prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                        resultFrameTupleAcc.reset(prevCurrentBuffer);
                        resultTidx = 0;
                    }
                }
            }

            if (advanceCursor) {
                invListTidx++;
                if (invListCursor.hasNext()) {
                    invListCursor.next();
                }
            }
        }

        // append remaining new elements from inverted list
        while (invListTidx < invListNumTuples) {
            ITupleReference invListTuple = invListCursor.getTuple();
            newBufIdx = appendTupleToNewResults(invListTuple, 1, newBufIdx);
            invListTidx++;
            if (invListCursor.hasNext()) {
                invListCursor.next();
            }
        }

        // append remaining elements from previous result set
        while (resultTidx < resultFrameTupleAcc.getTupleCount()) {

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                    resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
            newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);

            resultTidx++;
            if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                prevBufIdx++;
                if (prevBufIdx <= maxPrevBufIdx) {
                    prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                    resultFrameTupleAcc.reset(prevCurrentBuffer);
                    resultTidx = 0;
                }
            }
        }

        return newBufIdx;
    }

    protected int appendTupleToNewResults(ITupleReference tuple, int newCount, int newBufIdx) {
        ByteBuffer newCurrentBuffer = newResultBuffers.get(newBufIdx);

        if (!resultFrameTupleApp.hasSpace()) {
            newBufIdx++;
            if (newBufIdx >= newResultBuffers.size()) {
                newResultBuffers.add(ctx.allocateFrame());
            }
            newCurrentBuffer = newResultBuffers.get(newBufIdx);
            resultFrameTupleApp.reset(newCurrentBuffer, true);
        }

        // append key
        if (!resultFrameTupleApp.append(tuple.getFieldData(0), tuple.getFieldStart(0), invListKeyLength)) {
            throw new IllegalStateException();
        }

        // append new count
        if (!resultFrameTupleApp.append(newCount)) {
            throw new IllegalStateException();
        }

        resultFrameTupleApp.incrementTupleCount(1);

        currentNumResults++;

        return newBufIdx;
    }

    public IFrameTupleAccessor createResultFrameTupleAccessor() {
        return new FixedSizeFrameTupleAccessor(ctx.getFrameSize(), invListFieldsWithCount);
    }

    public ITupleReference createResultFrameTupleReference() {
        return new FixedSizeTupleReference(invListFieldsWithCount);
    }

    @Override
    public List<ByteBuffer> getResultBuffers() {
        return newResultBuffers;
    }

    @Override
    public int getNumValidResultBuffers() {
        return maxResultBufIdx + 1;
    }

    public int getOccurrenceThreshold() {
        return occurrenceThreshold;
    }

    public void printNewResults(int maxResultBufIdx, List<ByteBuffer> buffer) {
        StringBuffer strBuffer = new StringBuffer();
        for (int i = 0; i <= maxResultBufIdx; i++) {
            ByteBuffer testBuf = buffer.get(i);
            resultFrameTupleAcc.reset(testBuf);
            for (int j = 0; j < resultFrameTupleAcc.getTupleCount(); j++) {
                strBuffer.append(IntegerSerializerDeserializer.getInt(resultFrameTupleAcc.getBuffer().array(),
                        resultFrameTupleAcc.getFieldStartOffset(j, 0)) + ",");
                strBuffer.append(IntegerSerializerDeserializer.getInt(resultFrameTupleAcc.getBuffer().array(),
                        resultFrameTupleAcc.getFieldStartOffset(j, 1)) + " ");
            }
        }
        System.out.println(strBuffer.toString());
    }
}
