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

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.IIndexCursor;

public interface ILSMIndexCursor extends IIndexCursor {
    /**
     * @return the min tuple of the corresponding component's filter
     */
    ITupleReference getFilterMinTuple();

    /**
     *
     * @return the max tuple of the corresponding component's filter
     */
    ITupleReference getFilterMaxTuple();

    /**
     * Returns the result of the current SearchOperationCallback.proceed().
     * This method is used for the secondary-index searches.
     *
     * @return true if SearchOperationCallback.proceed() succeeded
     *         false otherwise
     */
    boolean getSearchOperationCallbackProceedResult();

}
