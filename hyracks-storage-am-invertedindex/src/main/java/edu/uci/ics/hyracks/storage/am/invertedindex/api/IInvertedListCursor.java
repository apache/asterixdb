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

package edu.uci.ics.hyracks.storage.am.invertedindex.api;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public interface IInvertedListCursor extends Comparable<IInvertedListCursor> {
    void reset(int startPageId, int endPageId, int startOff, int numElements);

    void pinPagesSync() throws HyracksDataException;

    void pinPagesAsync() throws HyracksDataException;

    void unpinPages() throws HyracksDataException;

    boolean hasNext();

    void next();

    ITupleReference getTuple();

    // getters
    int getNumElements();

    int getStartPageId();

    int getEndPageId();

    int getStartOff();

    // jump to a specific element
    void positionCursor(int elementIx);

    boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp);

    // for debugging
    String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException;

    String printCurrentElement(ISerializerDeserializer[] serdes) throws HyracksDataException;
}
