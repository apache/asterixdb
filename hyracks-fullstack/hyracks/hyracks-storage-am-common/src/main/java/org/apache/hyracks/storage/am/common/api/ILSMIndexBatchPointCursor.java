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

import java.util.BitSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.ISearchPredicate;

/**
 * A cursor that resolves a batch of point lookups in a single pass, reusing the same underlying
 * sample cursor across predicates instead of opening one cursor per key.
 */
public interface ILSMIndexBatchPointCursor {

    /**
     * Sets the predicate to evaluate on the next {@link #hasNextWithPredicate(BitSet)} call.
     * The same cursor instance is reused across predicates.
     *
     * @param predicate the search predicate to evaluate.
     */
    void setPredicate(ISearchPredicate predicate);

    /**
     * Advances the cursor over the current predicate's keys and records which of them were found.
     *
     * @param foundRecordsIndex bit set whose i-th bit is set if the i-th key in the batch was found.
     * @throws HyracksDataException if the underlying cursor fails.
     */
    void hasNextWithPredicate(BitSet foundRecordsIndex) throws HyracksDataException;
}
