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
package org.apache.hyracks.storage.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * This operation callback allows for arbitrary actions to be taken while traversing
 * an index structure. The {@link IModificationOperationCallback} will be called on
 * all modifying operations (e.g. insert, update, delete...) for all indexes.
 */
public interface IModificationOperationCallback {

    /**
     * This method is called on a tuple that is about to traverse an index's structure
     * (i.e. before any pages are pinned or latched).
     * The format of the tuple is the format that would be stored in the index itself.
     *
     * @param tuple
     *            the tuple that is about to be operated on
     */
    public void before(ITupleReference tuple) throws HyracksDataException;

    /**
     * This method is called on a tuple when a tuple with a matching key is found for the
     * current operation. It is possible that tuple is null, in which case no tuple with a
     * matching key was found.
     * When found is called, the leaf page where the tuple resides will be pinned and latched,
     * so blocking operations should be avoided.
     *
     * @param tuple
     *            a tuple with a matching key, otherwise null if none exists
     */
    public void found(ITupleReference before, ITupleReference after) throws HyracksDataException;
}
