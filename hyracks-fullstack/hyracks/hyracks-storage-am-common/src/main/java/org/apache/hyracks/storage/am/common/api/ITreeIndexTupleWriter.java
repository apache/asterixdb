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

package org.apache.hyracks.storage.am.common.api;

import java.nio.ByteBuffer;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ITreeIndexTupleWriter {
    public int writeTuple(ITupleReference tuple, ByteBuffer targetBuf, int targetOff);

    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff);

    public int bytesRequired(ITupleReference tuple);

    public int writeTupleFields(ITupleReference tuple, int startField, int numFields, byte[] targetBuf, int targetOff);

    public int bytesRequired(ITupleReference tuple, int startField, int numFields);

    // return a tuplereference instance that can read the tuple written by this
    // writer the main idea is that the format of the written tuple may not be the same
    // as the format written by this writer
    public ITreeIndexTupleReference createTupleReference();

    // This method is only used by the BTree leaf frame split method since tuples
    // in the LSM-BTree can be either matter or anti-matter tuples and we want
    // to calculate the size of all tuples in the frame.
    public int getCopySpaceRequired(ITupleReference tuple);

    /**
     * Method sets tuple writer to raise update-in-place bit
     *
     * @param isUpdated
     *            Value of update-in-place bit
     */
    default void setUpdated(boolean isUpdated) {
        // Dummy default: most tuple writers don't support update bit
    }
}
