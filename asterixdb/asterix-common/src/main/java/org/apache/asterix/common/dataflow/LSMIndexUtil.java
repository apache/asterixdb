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
package org.apache.asterix.common.dataflow;

import java.util.List;

import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;

public class LSMIndexUtil {

    private LSMIndexUtil() {
    }

    public static void checkAndSetFirstLSN(AbstractLSMIndex lsmIndex, ILogManager logManager)
            throws HyracksDataException {
        // If the index has an empty memory component, we need to set its first LSN (For soft checkpoint)
        if (lsmIndex.isCurrentMutableComponentEmpty()) {
            //prevent transactions from incorrectly setting the first LSN on a modified component by checking the index is still empty
            synchronized (lsmIndex.getOperationTracker()) {
                if (lsmIndex.isCurrentMutableComponentEmpty()) {
                    LSMIOOperationCallback ioOpCallback = (LSMIOOperationCallback) lsmIndex.getIOOperationCallback();
                    ioOpCallback.setFirstLsnForCurrentMemoryComponent(logManager.getAppendLSN());
                }
            }
        }
    }

    public static void waitFor(List<? extends ILSMIOOperation> ioOperations) throws HyracksDataException {
        for (int i = 0; i < ioOperations.size(); i++) {
            try {
                ioOperations.get(i).sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
            }
        }
    }
}
