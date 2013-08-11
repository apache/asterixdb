/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.pregelix.dataflow.util;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public class CopyUpdateUtil {

    public static void copyUpdate(SearchKeyTupleReference tempTupleReference, ITupleReference frameTuple,
            UpdateBuffer updateBuffer, ArrayTupleBuilder cloneUpdateTb, IIndexAccessor indexAccessor,
            IIndexCursor cursor, RangePredicate rangePred) throws HyracksDataException, IndexException {
        if (cloneUpdateTb.getSize() > 0) {
            int[] fieldEndOffsets = cloneUpdateTb.getFieldEndOffsets();
            int srcStart = fieldEndOffsets[0];
            int srcLen = fieldEndOffsets[1] - fieldEndOffsets[0]; // the updated vertex size
            int frSize = frameTuple.getFieldLength(1); // the vertex binary size in the leaf page
            if (srcLen <= frSize) {
                //doing in-place update if the vertex size is not larger than the original size, save the "real update" overhead
                System.arraycopy(cloneUpdateTb.getByteArray(), srcStart, frameTuple.getFieldData(1),
                        frameTuple.getFieldStart(1), srcLen);
                cloneUpdateTb.reset();
                return;
            }
            if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                tempTupleReference.reset(frameTuple.getFieldData(0), frameTuple.getFieldStart(0),
                        frameTuple.getFieldLength(0));
                //release the cursor/latch
                cursor.close();
                //batch update
                updateBuffer.updateIndex(indexAccessor);
                //try append the to-be-updated tuple again
                if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                    throw new HyracksDataException("cannot append tuple builder!");
                }
                //search again and recover the cursor
                cursor.reset();
                rangePred.setLowKey(tempTupleReference, false);
                rangePred.setHighKey(null, true);
                indexAccessor.search(cursor, rangePred);
            }
            cloneUpdateTb.reset();
        }
    }
}
