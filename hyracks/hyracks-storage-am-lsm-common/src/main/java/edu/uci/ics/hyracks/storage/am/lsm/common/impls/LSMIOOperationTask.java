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

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;

public class LSMIOOperationTask<T> extends FutureTask<T> {
    private final ILSMIOOperation operation;

    public LSMIOOperationTask(Callable<T> callable) {
        super(callable);
        this.operation = (ILSMIOOperation) callable;
    }

    public ILSMIOOperation getOperation() {
        return operation;
    }
}
