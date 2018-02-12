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

import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class TestLsmBtreeSearchCursor extends LSMBTreeSearchCursor {

    private final TestLsmBtree lsmBtree;

    public TestLsmBtreeSearchCursor(ILSMIndexOperationContext opCtx, TestLsmBtree lsmBtree) {
        super(opCtx);
        this.lsmBtree = lsmBtree;
    }

    @Override
    public void doNext() throws HyracksDataException {
        try {
            List<ITestOpCallback<Semaphore>> callbacks = lsmBtree.getSearchCallbacks();
            synchronized (callbacks) {
                for (ITestOpCallback<Semaphore> cb : callbacks) {
                    TestLsmBtree.callback(cb, lsmBtree.getSearchSemaphore());
                }
            }
            lsmBtree.getSearchSemaphore().acquire();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        super.doNext();
    }
}
