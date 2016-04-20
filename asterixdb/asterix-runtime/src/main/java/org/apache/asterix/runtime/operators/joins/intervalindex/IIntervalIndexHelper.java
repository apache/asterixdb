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
package org.apache.asterix.runtime.operators.joins.intervalindex;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public interface IIntervalIndexHelper {

    //    public void nextFrame(ByteBuffer buffer) throws HyracksDataException;
    public boolean addTupleToIndex(IFrameTupleAccessor accessor, int i, TuplePointer tuplePointer)
            throws HyracksDataException;

    public boolean addTupleToIndex(ITupleAccessor accessor, TuplePointer tuplePointer) throws HyracksDataException;

    public boolean addTupleEndToIndex(ITupleAccessor accessor, TuplePointer tuplePointer) throws HyracksDataException;

    public ITuplePointerAccessor createTuplePointerAccessor();

    public EndPointIndexItem next();

    public boolean remove(EndPointIndexItem i);

    public EndPointIndexItem top();

}
