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

package edu.uci.ics.hyracks.storage.am.common.api;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ITreeIndexTupleWriter {
    public int writeTuple(ITupleReference tuple, ByteBuffer targetBuf, int targetOff);
    
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff);

    public int bytesRequired(ITupleReference tuple);

    // TODO: change to byte[] as well.
    public int writeTupleFields(ITupleReference tuple, int startField, int numFields, ByteBuffer targetBuf,
            int targetOff);

    public int bytesRequired(ITupleReference tuple, int startField, int numFields);

    // return a tuplereference instance that can read the tuple written by this
    // writer
    // the main idea is that the format of the written tuple may not be the same
    // as the format written by this writer
    public ITreeIndexTupleReference createTupleReference();
}
