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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.IModificationOperationCallback;

public interface IExtendedModificationOperationCallback extends IModificationOperationCallback {
    /**
     * Called after the action taken in found, to take action on a tuple that is not part of the index
     * itself but is part of an ancillary structure that is updated alongside the index. An example would
     * be a simple statistic on the index that records the minimum and maximum values.
     *
     * @param after
     *            The tuple to feed to the ancilliary structure
     * @throws HyracksDataException
     */

    void after(ITupleReference after) throws HyracksDataException;
}
