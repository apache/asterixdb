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

package org.apache.hyracks.storage.am.btree.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

public interface IBTreeLeafFrame extends IBTreeFrame {
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference pageTuple, MultiComparator cmp,
            FindTupleMode ftm, FindTupleNoExactMatchPolicy ftp) throws HyracksDataException;

    public int findUpdateTupleIndex(ITupleReference tuple) throws HyracksDataException;

    public int findUpsertTupleIndex(ITupleReference tuple) throws HyracksDataException;

    /**
     * @param searchTuple
     *            the tuple to match
     * @param targetTupleIndex
     *            the index of the tuple to check
     * @return the tuple at targetTupleIndex if its keys match searchTuple's keys, otherwise null
     * @throws HyracksDataException
     */
    public ITupleReference getMatchingKeyTuple(ITupleReference searchTuple, int targetTupleIndex)
            throws HyracksDataException;

    public void setNextLeaf(int nextPage);

    public int getNextLeaf();

    void ensureCapacity(IBufferCache bufferCache, ITupleReference tuple, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException;
}
