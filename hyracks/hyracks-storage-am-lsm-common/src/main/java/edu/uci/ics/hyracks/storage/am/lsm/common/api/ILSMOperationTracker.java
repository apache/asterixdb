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
package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;

/**
 * This interface exposes methods for tracking and setting the status of operations for the purpose
 * of coordinating flushes/merges in {@link ILSMIndex}.
 * Note that 'operation' below refers to {@link IIndexAccessor} methods.
 * 
 * @author zheilbron
 */
public interface ILSMOperationTracker {

    /**
     * An {@link ILSMIndex} will call this method before an operation enters it,
     * i.e., before any latches are taken.
     * If tryOperation is true, and the operation would have to wait for a flush,
     * then this method does not block and returns false.
     * Otherwise, this method returns true, and the operation is considered 'active' in the index.
     */
    public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException;

    /**
     * An {@link ILSMIndex} will call this method after an operation has left the index,
     * i.e., after all relevant latches have been released.
     * After this method has been called, the operation is still considered 'active',
     * until the issuer of the operation declares it completed by calling completeOperation().
     */
    public void afterOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException;

    /**
     * This method must be called by whoever is requesting the index operation through an {@link IIndexAccessor}.
     * The use of this method indicates that the operation is no longer 'active'
     * for the purpose of coordinating flushes/merges.
     */
    public void completeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException;
}
