/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package org.apache.hyracks.storage.common.buffercache;

import org.apache.hyracks.api.exceptions.HyracksDataException;

@FunctionalInterface
public interface IFIFOPageQueue {

    /**
     * Put a page in the write queue
     *
     * @param page
     *            the page to be written
     * @param callback
     *            callback in case of a failure
     * @throws HyracksDataException
     *             if the callback has already failed. This indicates a failure writing a previous page
     *             in the same operation.
     *             Note: having this failure at this place removes the need to check for failures with
     *             every add() call in the bulk loader and so, we check per page given to disk rather
     *             than per tuple given to loader. At the same time, it allows the bulk load to fail early.
     */
    void put(ICachedPage page, IPageWriteFailureCallback callback) throws HyracksDataException;
}
