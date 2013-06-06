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
package edu.uci.ics.hyracks.storage.am.common;

import java.util.Random;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;

public enum TestOperationCallback implements ISearchOperationCallback, IModificationOperationCallback {
    INSTANCE;

    private static final int RANDOM_SEED = 50;
    private final Random random = new Random();

    private TestOperationCallback() {
        random.setSeed(RANDOM_SEED);
    }

    @Override
    public boolean proceed(ITupleReference tuple) {
        // Always fail
        return false;
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
