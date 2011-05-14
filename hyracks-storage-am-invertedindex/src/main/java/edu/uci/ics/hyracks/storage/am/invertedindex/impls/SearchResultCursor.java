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

import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearcher;

public class SearchResultCursor implements IInvertedIndexResultCursor {

    private List<ByteBuffer> resultBuffers;
    private IFrameTupleAccessor fta;
    private FixedSizeTupleReference resultTuple;
    private int numResultBuffers;
    private int currentBufferIndex = 0;
    private int tupleIndex = 0;    
    
    public SearchResultCursor(IFrameTupleAccessor fta, ITupleReference resultTuple) {
        this.fta = fta;
        this.resultTuple = (FixedSizeTupleReference)resultTuple;
    }
    
    @Override
    public boolean hasNext() {
        if (currentBufferIndex < numResultBuffers && tupleIndex < fta.getTupleCount())
            return true;
        else
            return false;
    }

    @Override
    public void next() {    	
    	resultTuple.reset(fta.getBuffer().array(), fta.getTupleStartOffset(tupleIndex));
        tupleIndex++;
        if(tupleIndex >= fta.getTupleCount()) {            
        	if(currentBufferIndex + 1 < numResultBuffers) {
                currentBufferIndex++;
                fta.reset(resultBuffers.get(currentBufferIndex));
                tupleIndex = 0;
            }
    	}
    }

    @Override
    public ITupleReference getTuple() {
        return resultTuple;
    }

    @Override
    public void reset(IInvertedIndexSearcher invIndexSearcher) {        
        currentBufferIndex = 0;
        tupleIndex = 0;
        resultBuffers = invIndexSearcher.getResultBuffers();
        numResultBuffers = invIndexSearcher.getNumValidResultBuffers();
        if(numResultBuffers > 0) {
            fta.reset(resultBuffers.get(0));
        }
    }
}
