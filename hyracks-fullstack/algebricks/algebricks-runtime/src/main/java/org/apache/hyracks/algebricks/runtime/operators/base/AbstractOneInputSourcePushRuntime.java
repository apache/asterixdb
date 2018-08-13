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
package org.apache.hyracks.algebricks.runtime.operators.base;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractOneInputSourcePushRuntime extends AbstractOneInputPushRuntime {

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // nextFrame will never be called on this runtime
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() throws HyracksDataException {
        // flush will never be called on this runtime
        throw new UnsupportedOperationException();
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        // setInputRecordDescriptor will never be called on this runtime since it has no input
        throw new UnsupportedOperationException();
    }
}
