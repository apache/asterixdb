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

package org.apache.hyracks.storage.am.common.tuples;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * A wrapper class to wrap ITupleReference into IFrameTupleReference, as the latter
 * is used by ITupleFilter
 */
public final class ReferenceFrameTupleReference implements IFrameTupleReference {

    private ITupleReference tuple;

    public void reset(ITupleReference tuple) {
        this.tuple = tuple;
    }

    @Override
    public int getFieldCount() {
        return tuple.getFieldCount();
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return tuple.getFieldData(fIdx);
    }

    @Override
    public int getFieldStart(int fIdx) {
        return tuple.getFieldStart(fIdx);
    }

    @Override
    public int getFieldLength(int fIdx) {
        return tuple.getFieldLength(fIdx);
    }

    @Override
    public IFrameTupleAccessor getFrameTupleAccessor() {
        throw new UnsupportedOperationException(
                "getFrameTupleAccessor is not supported by ReferenceFrameTupleReference");
    }

    @Override
    public int getTupleIndex() {
        throw new UnsupportedOperationException("getTupleIndex is not supported by ReferenceFrameTupleReference");
    }
}
