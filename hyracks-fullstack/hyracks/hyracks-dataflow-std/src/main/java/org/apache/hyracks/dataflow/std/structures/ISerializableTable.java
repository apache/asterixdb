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
package org.apache.hyracks.dataflow.std.structures;

import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;

public interface ISerializableTable {

    boolean insert(int entry, TuplePointer tuplePointer) throws HyracksDataException;

    void delete(int entry);

    boolean getTuplePointer(int entry, int offset, TuplePointer tuplePointer);

    /**
     * Returns the byte size of entire frames that are currently allocated to the table.
     */
    int getCurrentByteSize();

    int getTupleCount();

    int getTupleCount(int entry);

    void reset();

    void close();

    boolean isGarbageCollectionNeeded();

    /**
     * Collects garbages in the given table, if any. For example, compacts the table by
     * removing the garbage created by internal migration or lazy deletion operation.
     * The desired result of this method is a compacted table without any garbage (no wasted space).
     *
     * @param bufferAccessor:
     *            required to access the real tuple to calculate the original hash value
     * @param tpc:
     *            hash function
     * @return the number of frames that are reclaimed.
     * @throws HyracksDataException
     */
    int collectGarbage(ITuplePointerAccessor bufferAccessor, ITuplePartitionComputer tpc) throws HyracksDataException;

    /**
     * Prints out the internal information of this table.
     */
    String printInfo();

    /**
     * Returns the number of entries of this table.
     * @return the number entries.
     */
    int getTableSize();
}
