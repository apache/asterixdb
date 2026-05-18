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
package org.apache.hyracks.storage.am.lsm.vector.multithread;

import java.util.List;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.IIndexAccessor;

/**
 * Worker thread that inserts pre-generated vector tuples into an LSMVTree index.
 * Each worker holds its own IIndexAccessor (and thus its own OpContext with frame objects)
 * to avoid sharing mutable state across threads.
 */
public class LSMVTreeTestWorker implements Runnable {

    private final IIndexAccessor accessor;
    private final List<ITupleReference> tuples;
    private volatile Exception exception;

    public LSMVTreeTestWorker(IIndexAccessor accessor, List<ITupleReference> tuples) {
        this.accessor = accessor;
        this.tuples = tuples;
    }

    @Override
    public void run() {
        try {
            for (ITupleReference tuple : tuples) {
                accessor.insert(tuple);
            }
        } catch (Exception e) {
            this.exception = e;
        }
    }

    public Exception getException() {
        return exception;
    }
}
