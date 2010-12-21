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

import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexResultCursor;

public class ListResultCursor implements IInvertedIndexResultCursor {

    private List<ByteBuffer> resultBuffers;
    private int numResultBuffers;
    private int currentPos = 0;

    public void setResults(List<ByteBuffer> resultBuffers, int numResultBuffers) {
        this.resultBuffers = resultBuffers;
        this.numResultBuffers = numResultBuffers;
        reset();
    }

    @Override
    public boolean hasNext() {
        if (currentPos + 1 < numResultBuffers)
            return true;
        else
            return false;
    }

    @Override
    public void next() {
        currentPos++;
    }

    @Override
    public ByteBuffer getBuffer() {
        return resultBuffers.get(currentPos);
    }

    @Override
    public void reset() {
        currentPos = -1;
    }
}
