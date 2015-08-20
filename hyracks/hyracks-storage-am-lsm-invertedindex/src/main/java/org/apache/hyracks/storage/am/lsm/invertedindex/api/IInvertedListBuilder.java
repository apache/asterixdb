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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IInvertedListBuilder {
    public boolean startNewList(ITupleReference tuple, int numTokenFields);

    // returns true if successfully appended
    // returns false if not enough space in targetBuf
    public boolean appendElement(ITupleReference tuple, int numTokenFields, int numElementFields);

    public void setTargetBuffer(byte[] targetBuf, int startPos);

    public int getListSize();

    public int getPos();
}
