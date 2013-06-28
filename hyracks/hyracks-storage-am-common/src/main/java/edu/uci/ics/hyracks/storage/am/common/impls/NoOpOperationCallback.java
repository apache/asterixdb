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

package edu.uci.ics.hyracks.storage.am.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;

/**
 * Dummy operation callback that simply does nothing.
 */
public enum NoOpOperationCallback implements IModificationOperationCallback, ISearchOperationCallback {
    INSTANCE;

    @Override
    public boolean proceed(ITupleReference tuple) {
        return true;
    }

    @Override
    public void reconcile(ITupleReference tuple) {
        // Do nothing.
    }

    @Override
    public void before(ITupleReference tuple) {
        // Do nothing.        
    }

    @Override
    public void found(ITupleReference before, ITupleReference after) {
        // Do nothing.        
    }

    @Override
    public void cancel(ITupleReference tuple) {
        // Do nothing.
    }

    @Override
    public void complete(ITupleReference tuple) throws HyracksDataException {
        // Do nothing.
    }
}
