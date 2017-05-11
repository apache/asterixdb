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
package org.apache.hyracks.storage.am.lsm.common.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;

/**
 * This interface exposes methods for tracking and setting the status of operations for the purpose
 * of coordinating flushes/merges in {@link ILSMIndex}.
 * Note that 'operation' below refers to {@link IIndexAccessor} methods.
 */
public interface ILSMOperationTracker {

    /**
     * An {@link ILSMIndex} will call this method before an operation enters it,
     * i.e., before any latches are taken.
     * If tryOperation is true, and the operation would have to wait for a flush,
     * then this method does not block and returns false.
     * Otherwise, this method returns true, and the operation is considered 'active' in the index.
     */
    void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException;

    /**
     * An {@link ILSMIndex} will call this method after an operation has left the index,
     * i.e., after all relevant latches have been released.
     * After this method has been called, the operation is still considered 'active',
     * until the issuer of the operation declares it completed by calling completeOperation().
     */
    void afterOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException;

    /**
     * This method must be called by whoever is requesting the index operation through an {@link IIndexAccessor}.
     * The use of this method indicates that the operation is no longer 'active'
     * for the purpose of coordinating flushes/merges.
     */
    void completeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException;
}
