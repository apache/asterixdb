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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.common.IIndex;

public interface IInvertedIndex extends IIndex {
    IInvertedListCursor createInvertedListCursor();

    void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference searchKey, IIndexOperationContext ictx)
            throws HyracksDataException;

    ITypeTraits[] getInvListTypeTraits();

    IBinaryComparatorFactory[] getInvListCmpFactories();

    ITypeTraits[] getTokenTypeTraits();

    IBinaryComparatorFactory[] getTokenCmpFactories();

    /**
     * Purge the index files out of the buffer cache.
     * Can only be called if the caller is absolutely sure the files don't contain dirty pages
     *
     * @throws HyracksDataException
     *             if the index is active
     */
    void purge() throws HyracksDataException;
}
