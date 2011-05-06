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

package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.uci.ics.fuzzyjoin.tokenizer.IBinaryTokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.IToken;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.TreeIndexOp;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;

public class TOccurrenceSearcher {

    protected final IHyracksStageletContext ctx;
    protected final FixedSizeFrameTupleAppender resultFrameTupleApp;
    protected final FixedSizeFrameTupleAccessor resultFrameTupleAcc;
    protected final FixedSizeTupleReference resultTuple;
    protected final int invListKeyLength;
    protected int currentNumResults;
    
    protected List<ByteBuffer> newResultBuffers = new ArrayList<ByteBuffer>();
    protected List<ByteBuffer> prevResultBuffers = new ArrayList<ByteBuffer>();
    protected List<ByteBuffer> swap = null;
    protected final ListResultCursor resultCursor = new ListResultCursor();
    protected int maxResultBufIdx = 0;

    protected final IBTreeLeafFrame leafFrame;
    protected final IBTreeInteriorFrame interiorFrame;
    protected final IBTreeCursor btreeCursor;
    protected final FrameTupleReference searchKey = new FrameTupleReference();
    protected final RangePredicate btreePred = new RangePredicate(true, null, null, true, true, null, null);
        
    protected final InvertedIndex invIndex;    
    protected final IBinaryTokenizer queryTokenizer;    
    protected int occurrenceThreshold;
    
    protected final int cursorCacheSize = 10;
    protected List<IInvertedListCursor> invListCursorCache = new ArrayList<IInvertedListCursor>(cursorCacheSize);
    protected List<IInvertedListCursor> invListCursors = new ArrayList<IInvertedListCursor>(cursorCacheSize);
    
    public TOccurrenceSearcher(IHyracksStageletContext ctx, InvertedIndex invIndex, IBinaryTokenizer queryTokenizer) {
        this.ctx = ctx;
        this.invIndex = invIndex;
        this.queryTokenizer = queryTokenizer;

        leafFrame = invIndex.getBTree().getLeafFrameFactory().getFrame();
        interiorFrame = invIndex.getBTree().getInteriorFrameFactory().getFrame();

        btreeCursor = new RangeSearchCursor(leafFrame);
        ITypeTrait[] invListFields = invIndex.getInvListElementCmp().getTypeTraits();
        ITypeTrait[] invListFieldsWithCount = new TypeTrait[invListFields.length + 1];
        int tmp = 0;
        for(int i = 0; i < invListFields.length; i++) {
            invListFieldsWithCount[i] = invListFields[i];
            tmp += invListFields[i].getStaticallyKnownDataLength();
        }
        // using an integer for counting occurrences
        invListFieldsWithCount[invListFields.length] = new TypeTrait(4);
        invListKeyLength = tmp;
        
        resultFrameTupleApp = new FixedSizeFrameTupleAppender(ctx.getFrameSize(), invListFieldsWithCount);
        resultFrameTupleAcc = new FixedSizeFrameTupleAccessor(ctx.getFrameSize(), invListFieldsWithCount);
        resultTuple = new FixedSizeTupleReference(invListFieldsWithCount);
        newResultBuffers.add(ctx.allocateFrame());
        prevResultBuffers.add(ctx.allocateFrame());

        MultiComparator searchCmp = invIndex.getBTree().getMultiComparator();
        btreePred.setLowKeyComparator(searchCmp);
        btreePred.setHighKeyComparator(searchCmp);
        btreePred.setLowKey(searchKey, true);
        btreePred.setHighKey(searchKey, true);                

        // pre-create cursor objects
        for (int i = 0; i < cursorCacheSize; i++) {
            invListCursorCache.add(new FixedSizeElementInvertedListCursor(invIndex.getBufferCache(), invIndex
                    .getInvListsFileId(), invIndex.getInvListElementCmp().getTypeTraits()));
        }
        
        currentNumResults = 0;
    }

    public void reset() {
        for(ByteBuffer b : newResultBuffers) {
            resultFrameTupleApp.reset(b, true);
        }
        for(ByteBuffer b : prevResultBuffers) {
            resultFrameTupleApp.reset(b, true);
        }
        currentNumResults = 0;
    }
        
    public void search(ITupleReference queryTuple, int queryFieldIndex) throws Exception {
        
        RecordDescriptor queryTokenRecDesc = new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });

        ArrayTupleBuilder queryTokenBuilder = new ArrayTupleBuilder(queryTokenRecDesc.getFields().length);
        DataOutput queryTokenDos = queryTokenBuilder.getDataOutput();
        FrameTupleAppender queryTokenAppender = new FrameTupleAppender(ctx.getFrameSize());
        ByteBuffer queryTokenFrame = ctx.allocateFrame();
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

        maxResultBufIdx = 0;

        // expand cursor cache if necessary
        if (numQueryTokens > invListCursorCache.size()) {
            int diff = numQueryTokens - invListCursorCache.size();
            for (int i = 0; i < diff; i++) {
                invListCursorCache.add(new FixedSizeElementInvertedListCursor(invIndex.getBufferCache(), invIndex
                        .getInvListsFileId(), invIndex.getInvListElementCmp().getTypeTraits()));
            }
        }
        
        BTreeOpContext btreeOpCtx = invIndex.getBTree().createOpContext(TreeIndexOp.TI_SEARCH, leafFrame,
                interiorFrame, null);
        
        invListCursors.clear();
        for (int i = 0; i < numQueryTokens; i++) {
            searchKey.reset(queryTokenAccessor, i);
            invIndex.openCursor(btreeCursor, btreePred, btreeOpCtx, invListCursorCache.get(i));
            invListCursors.add(invListCursorCache.get(i));            
        }
        Collections.sort(invListCursors);
        
        /*
        for(int i = 0; i < numQueryTokens; i++) {
            System.out.println("SIZE: " + i + " " + invListCursors.get(i).getNumElements());
        }
        */
        
        occurrenceThreshold = numQueryTokens;
        
        int numPrefixTokens = numQueryTokens - occurrenceThreshold + 1;
        
        int maxPrevBufIdx = mergePrefixLists(numPrefixTokens, numQueryTokens);        
        maxPrevBufIdx = mergeSuffixLists(numPrefixTokens, numQueryTokens, maxPrevBufIdx);        
        
        /*
        StringBuffer strBuffer = new StringBuffer();
        for(int i = 0; i <= maxPrevBufIdx; i++) {
            ByteBuffer testBuf = newResultBuffers.get(i);
            resultFrameTupleAcc.reset(testBuf);
            for(int j = 0; j < resultFrameTupleAcc.getTupleCount(); j++) {
                strBuffer.append(IntegerSerializerDeserializer.getInt(resultFrameTupleAcc.getBuffer().array(), resultFrameTupleAcc.getFieldStartOffset(j, 0)) + ",");
                strBuffer.append(IntegerSerializerDeserializer.getInt(resultFrameTupleAcc.getBuffer().array(), resultFrameTupleAcc.getFieldStartOffset(j, 1)) + " ");                
            }            
        }
        System.out.println(strBuffer.toString());     
        */   
        
    }        
        
    protected int mergePrefixLists(int numPrefixTokens, int numQueryTokens) throws IOException {
        int maxPrevBufIdx = 0;
        for(int i = 0; i < numPrefixTokens; i++) {
            swap = prevResultBuffers;
            prevResultBuffers = newResultBuffers;
            newResultBuffers = swap;
            currentNumResults = 0;
                        
            invListCursors.get(i).pinPagesSync();
            maxPrevBufIdx = mergePrefixList(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx, newResultBuffers);
            invListCursors.get(i).unpinPages();        
        }          
                
        return maxPrevBufIdx;
    }
        
    protected int mergeSuffixLists(int numPrefixTokens, int numQueryTokens, int maxPrevBufIdx) throws IOException {
        for(int i = numPrefixTokens; i < numQueryTokens; i++) {
            swap = prevResultBuffers;
            prevResultBuffers = newResultBuffers;
            newResultBuffers = swap;            
                        
            invListCursors.get(i).pinPagesSync();
            int numInvListElements = invListCursors.get(i).getNumElements();
            // should we binary search the next list or should we sort-merge it?
            if(currentNumResults * Math.log(numInvListElements) < currentNumResults + numInvListElements) {
                //System.out.println("PROBING LIST:  " + i);
                maxPrevBufIdx = mergeSuffixListProbe(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx, newResultBuffers, i, numQueryTokens);
            }
            else {
                //System.out.println("SCANNING LIST: " + i);
                maxPrevBufIdx = mergeSuffixListScan(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx, newResultBuffers, i, numQueryTokens);
            }
            invListCursors.get(i).unpinPages();        
        }                
        return maxPrevBufIdx;                
    }
    
    protected int mergeSuffixListProbe(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers, int maxPrevBufIdx, List<ByteBuffer> newResultBuffers, int invListIx, int numQueryTokens) throws IOException {
        
        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);
                
        int resultTidx = 0; 
        
        currentNumResults = 0;
        
        MultiComparator invListCmp = invIndex.getInvListElementCmp();
        
        resultFrameTupleAcc.reset(prevCurrentBuffer);
        resultFrameTupleApp.reset(newCurrentBuffer, true);
        
        while(resultTidx < resultFrameTupleAcc.getTupleCount()) {
            
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));            
            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0), resultTuple.getFieldStart(resultTuple.getFieldCount()-1)); 

            if(invListCursor.containsKey(resultTuple, invListCmp)) {
                count++;
                newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
            }
            else {
                if(count + numQueryTokens - invListIx > occurrenceThreshold) {
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
    
    protected int mergeSuffixListScan(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers, int maxPrevBufIdx, List<ByteBuffer> newResultBuffers, int invListIx, int numQueryTokens) throws IOException {
        
        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);
        
        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0; 
        
        currentNumResults = 0;
        
        MultiComparator invListCmp = invIndex.getInvListElementCmp();
        
        resultFrameTupleAcc.reset(prevCurrentBuffer);
        resultFrameTupleApp.reset(newCurrentBuffer, true);
        
        while(invListCursor.hasNext() && resultTidx < resultFrameTupleAcc.getTupleCount()) {
            
            if(advanceCursor) invListCursor.next();
            
            ITupleReference invListTuple = invListCursor.getTuple();
            
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));            
            
            int cmp = invListCmp.compare(invListTuple, resultTuple);
            if (cmp == 0) {
                int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0), resultTuple.getFieldStart(resultTuple.getFieldCount()-1)) + 1;
                newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);                
                advanceCursor = true;
                advancePrevResult = true;
            } else {
                if (cmp < 0) {                    
                    advanceCursor = true;
                    advancePrevResult = false;
                } else {
                    int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0), resultTuple.getFieldStart(resultTuple.getFieldCount()-1));                    
                    if(count + numQueryTokens - invListIx > occurrenceThreshold) {                    
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
        }
                
        // append remaining elements from previous result set
        //if(resultTidx < resultFrameTupleAcc.getTupleCount()) System.out.println("APPENDING FROM RESULTS");
        while(resultTidx < resultFrameTupleAcc.getTupleCount()) {                        
            
            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0), resultTuple.getFieldStart(resultTuple.getFieldCount()-1));
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
    
    protected int mergePrefixList(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers, int maxPrevBufIdx, List<ByteBuffer> newResultBuffers) throws IOException {                
        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);
        
        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0; 
        
        MultiComparator invListCmp = invIndex.getInvListElementCmp();
        
        resultFrameTupleAcc.reset(prevCurrentBuffer);
        resultFrameTupleApp.reset(newCurrentBuffer, true);
        
        while(invListCursor.hasNext() && resultTidx < resultFrameTupleAcc.getTupleCount()) {
            
            if(advanceCursor) invListCursor.next();
            
            ITupleReference invListTuple = invListCursor.getTuple();
            
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));            
            
            int cmp = invListCmp.compare(invListTuple, resultTuple);
            if (cmp == 0) {
                int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0), resultTuple.getFieldStart(resultTuple.getFieldCount()-1)) + 1;
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
                    int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0), resultTuple.getFieldStart(resultTuple.getFieldCount()-1));
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
        }
        
        // append remaining new elements from inverted list
        //if(invListCursor.hasNext()) System.out.println("APPENDING FROM INV LIST");        
        while(invListCursor.hasNext()) {            
            invListCursor.next();            
            ITupleReference invListTuple = invListCursor.getTuple();
            newBufIdx = appendTupleToNewResults(invListTuple, 1, newBufIdx);       
        }
        
        // append remaining elements from previous result set
        //if(resultTidx < resultFrameTupleAcc.getTupleCount()) System.out.println("APPENDING FROM RESULTS");
        while(resultTidx < resultFrameTupleAcc.getTupleCount()) {                        
            
            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0), resultTuple.getFieldStart(resultTuple.getFieldCount()-1));
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
    
    protected int appendTupleToNewResults(ITupleReference tuple, int newCount, int newBufIdx) throws IOException {                        
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
        if (!resultFrameTupleApp.append(tuple.getFieldData(0), tuple.getFieldStart(0), invListKeyLength) ) {
            throw new IllegalStateException();
        }
        
        // append new count
        if (!resultFrameTupleApp.append(newCount) ) {
            throw new IllegalStateException();
        }

        resultFrameTupleApp.incrementTupleCount(1);

        currentNumResults++;
        
        return newBufIdx;
    }
    
   
    /*
    private int appendTupleToNewResults(IBTreeCursor btreeCursor, int newBufIdx) throws IOException {
        ByteBuffer newCurrentBuffer = newResultBuffers.get(newBufIdx);

        ITupleReference tuple = btreeCursor.getTuple();
        resultTupleBuilder.reset();
        DataOutput dos = resultTupleBuilder.getDataOutput();
        for (int i = 0; i < numValueFields; i++) {
            int fIdx = numKeyFields + i;
            dos.write(tuple.getFieldData(fIdx), tuple.getFieldStart(fIdx), tuple.getFieldLength(fIdx));
            resultTupleBuilder.addFieldEndOffset();
        }

        if (!resultTupleAppender.append(resultTupleBuilder.getFieldEndOffsets(), resultTupleBuilder.getByteArray(), 0,
                resultTupleBuilder.getSize())) {
            newBufIdx++;
            if (newBufIdx >= newResultBuffers.size()) {
                newResultBuffers.add(ctx.allocateFrame());
            }
            newCurrentBuffer = newResultBuffers.get(newBufIdx);
            resultTupleAppender.reset(newCurrentBuffer, true);
            if (!resultTupleAppender.append(resultTupleBuilder.getFieldEndOffsets(), resultTupleBuilder.getByteArray(),
                    0, resultTupleBuilder.getSize())) {
                throw new IllegalStateException();
            }
        }

        return newBufIdx;
    }
    */

    
    /*
    private int mergeInvList(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers, int maxPrevBufIdx, List<ByteBuffer> newResultBuffers) throws IOException, Exception {

        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);

        resultTupleBuilder.reset();
        resultTupleAppender.reset(newCurrentBuffer, true);
        resultFrameAccessor.reset(prevCurrentBuffer);

        // WARNING: not very efficient but good enough for the first cut
        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0;

        while ((!advanceCursor || invListCursor.hasNext()) && prevBufIdx <= maxPrevBufIdx
                && resultTidx < resultFrameAccessor.getTupleCount()) {

            if (advanceCursor)
                invListCursor.next();
            
            ICachedPage invListPage = invListCursor.getPage();
            int invListOff = invListCursor.getOffset();
            
            int cmp = 0;
            int valueFields = 1;
            for (int i = 0; i < valueFields; i++) {
                                                
                int tupleFidx = numKeyFields + i;
                cmp = valueCmps[i].compare(tuple.getFieldData(tupleFidx), tuple.getFieldStart(tupleFidx),
                        tuple.getFieldLength(tupleFidx), resultFrameAccessor.getBuffer().array(),
                        resultFrameAccessor.getTupleStartOffset(resultTidx) + resultFrameAccessor.getFieldSlotsLength()
                                + resultFrameAccessor.getFieldStartOffset(resultTidx, i),
                        resultFrameAccessor.getFieldLength(resultTidx, i));
                if (cmp != 0)
                    break;
            }

            // match found
            if (cmp == 0) {
                newBufIdx = appendTupleToNewResults(btreeCursor, newBufIdx);

                advanceCursor = true;
                advancePrevResult = true;
            } else {
                if (cmp < 0) {
                    advanceCursor = true;
                    advancePrevResult = false;
                } else {
                    advanceCursor = false;
                    advancePrevResult = true;
                }
            }

            if (advancePrevResult) {
                resultTidx++;
                if (resultTidx >= resultFrameAccessor.getTupleCount()) {
                    prevBufIdx++;
                    if (prevBufIdx <= maxPrevBufIdx) {
                        prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                        resultFrameAccessor.reset(prevCurrentBuffer);
                        resultTidx = 0;
                    }
                }
            }
        }

        return newBufIdx;
    }
      
    private int appendTupleToNewResults(IBTreeCursor btreeCursor, int newBufIdx) throws IOException {
        ByteBuffer newCurrentBuffer = newResultBuffers.get(newBufIdx);

        ITupleReference tuple = btreeCursor.getTuple();
        resultTupleBuilder.reset();
        DataOutput dos = resultTupleBuilder.getDataOutput();
        for (int i = 0; i < numValueFields; i++) {
            int fIdx = numKeyFields + i;
            dos.write(tuple.getFieldData(fIdx), tuple.getFieldStart(fIdx), tuple.getFieldLength(fIdx));
            resultTupleBuilder.addFieldEndOffset();
        }

        if (!resultTupleAppender.append(resultTupleBuilder.getFieldEndOffsets(), resultTupleBuilder.getByteArray(), 0,
                resultTupleBuilder.getSize())) {
            newBufIdx++;
            if (newBufIdx >= newResultBuffers.size()) {
                newResultBuffers.add(ctx.allocateFrame());
            }
            newCurrentBuffer = newResultBuffers.get(newBufIdx);
            resultTupleAppender.reset(newCurrentBuffer, true);
            if (!resultTupleAppender.append(resultTupleBuilder.getFieldEndOffsets(), resultTupleBuilder.getByteArray(),
                    0, resultTupleBuilder.getSize())) {
                throw new IllegalStateException();
            }
        }

        return newBufIdx;
    }
    */

    /*
     * @Override public IInvertedIndexResultCursor getResultCursor() {
     * resultCursor.setResults(newResultBuffers, maxResultBufIdx + 1); return
     * resultCursor; }
     */
}
