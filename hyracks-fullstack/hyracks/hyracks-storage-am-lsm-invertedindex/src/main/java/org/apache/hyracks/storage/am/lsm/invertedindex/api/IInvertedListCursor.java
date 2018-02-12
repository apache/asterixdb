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

package org.apache.hyracks.storage.am.lsm.invertedindex.api;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.MultiComparator;

public interface IInvertedListCursor extends Comparable<IInvertedListCursor> {
    public void reset(int startPageId, int endPageId, int startOff, int numElements);

    public void pinPages() throws HyracksDataException;

    public void unpinPages() throws HyracksDataException;

    public boolean hasNext() throws HyracksDataException;

    public void next() throws HyracksDataException;

    public ITupleReference getTuple();

    // getters
    public int size() throws HyracksDataException;

    public int getStartPageId();

    public int getEndPageId();

    public int getStartOff();

    public boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp) throws HyracksDataException;

    // for debugging
    @SuppressWarnings("rawtypes")
    public String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException;

    @SuppressWarnings("rawtypes")
    public String printCurrentElement(ISerializerDeserializer[] serdes) throws HyracksDataException;
}
