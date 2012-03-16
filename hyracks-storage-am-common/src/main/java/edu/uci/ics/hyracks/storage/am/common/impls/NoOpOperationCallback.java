/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallback;

/**
 * Dummy operation callback that simply does nothing. Mainly, intended to be
 * used in non-transaction access method testing.
 * 
 */
public class NoOpOperationCallback implements IOperationCallback {

    public static IOperationCallback INSTANCE = new NoOpOperationCallback();
    
    @Override
    public void pre(ITupleReference tuple) {
        
    }

    @Override
    public void post(ITupleReference tuple) {
    }
}
