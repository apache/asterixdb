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
package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;

public enum NoOpIOOperationCallback implements ILSMIOOperationCallback, ILSMIOOperationCallbackProvider, ILSMIOOperationCallbackFactory {
    INSTANCE;

    @Override
    public void beforeOperation(LSMOperationType opType) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void afterOperation(LSMOperationType opType, List<ILSMComponent> oldComponents, ILSMComponent newComponent)
            throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void afterFinalize(LSMOperationType opType, ILSMComponent newComponent) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public ILSMIOOperationCallback getIOOperationCallback(ILSMIndex index) {
        return INSTANCE;
    }

    @Override
    public ILSMIOOperationCallback createIOOperationCallback() {
        return INSTANCE;
    }

    @Override
    public void setNumOfMutableComponents(int count) {
        // Do nothing.
    }
}
