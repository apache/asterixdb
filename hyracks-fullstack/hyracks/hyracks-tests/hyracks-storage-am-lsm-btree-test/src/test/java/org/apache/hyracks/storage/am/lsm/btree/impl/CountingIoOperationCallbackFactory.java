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
package org.apache.hyracks.storage.am.lsm.btree.impl;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.IResource;

public class CountingIoOperationCallbackFactory implements ILSMIOOperationCallbackFactory {

    private static final long serialVersionUID = 1L;
    public static int STARTING_INDEX = 0;

    @Override
    public void initialize(INCServiceContext ncCtx, IResource resource) {
        // No op
    }

    @Override
    public ILSMIOOperationCallback createIoOpCallback(ILSMIndex index) throws HyracksDataException {
        return new CountingIoOperationCallback();
    }

    @Override
    public int getCurrentMemoryComponentIndex() throws HyracksDataException {
        return STARTING_INDEX;
    }

}
