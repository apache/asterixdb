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
package org.apache.asterix.runtime.operators.joins.interval.utils.memory;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;

public abstract class AbstractTupleCursor<T> implements ITupleCursor<T> {

    public static final int UNSET = -2;
    public static final int INITIALIZED = -1;
    public int tupleId = UNSET;
    protected IFrameTupleAccessor accessor;

    @Override
    public void next() {
        ++tupleId;
    }

    @Override
    public IFrameTupleAccessor getAccessor() {
        return accessor;
    }

    @Override
    public int getTupleId() {
        return tupleId;
    }
}
