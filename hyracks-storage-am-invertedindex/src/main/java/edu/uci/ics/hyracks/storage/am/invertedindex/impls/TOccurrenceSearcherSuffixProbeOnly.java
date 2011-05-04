package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.fuzzyjoin.tokenizer.IBinaryTokenizer;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;

public class TOccurrenceSearcherSuffixProbeOnly extends TOccurrenceSearcher {

    public TOccurrenceSearcherSuffixProbeOnly(IHyracksStageletContext ctx, InvertedIndex invIndex,
            IBinaryTokenizer queryTokenizer) {
        super(ctx, invIndex, queryTokenizer);
    }
    
    protected int mergeSuffixLists(int numPrefixTokens, int numQueryTokens, int maxPrevBufIdx) throws IOException {
        for(int i = numPrefixTokens; i < numQueryTokens; i++) {
            swap = prevResultBuffers;
            prevResultBuffers = newResultBuffers;
            newResultBuffers = swap;
            currentNumResults = 0;
            
            invListCursors.get(i).pinPagesSync();
            maxPrevBufIdx = mergeSuffixListProbe(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx, newResultBuffers, i, numQueryTokens);
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
}
