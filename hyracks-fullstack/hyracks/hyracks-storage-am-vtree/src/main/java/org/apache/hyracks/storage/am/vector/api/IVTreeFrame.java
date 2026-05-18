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

package org.apache.hyracks.storage.am.vector.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;

/**
 * Base interface for all VTree frames.
 * <p>
 * Not thread-safe: an instance wraps one pinned page and is confined to a single operation context.
 */
public interface IVTreeFrame extends ITreeIndexFrame {

    /**
     * Appends {@code tuple} at the end of the page's sorted run. Precondition: {@code tuple}'s
     * ordering key is {@code >=} that of the last tuple already on the page, so appending keeps the
     * page sorted. Callers building a page in ascending key order use this to avoid a per-insert
     * position search.
     */
    void insertSorted(ITupleReference tuple) throws HyracksDataException;
}
