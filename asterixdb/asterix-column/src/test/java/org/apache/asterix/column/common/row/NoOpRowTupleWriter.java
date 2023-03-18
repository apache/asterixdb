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
package org.apache.asterix.column.common.row;

import java.nio.ByteBuffer;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;

public class NoOpRowTupleWriter implements ITreeIndexTupleWriter {

    @Override
    public int writeTuple(ITupleReference tuple, ByteBuffer targetBuf, int targetOff) {
        return 0;
    }

    @Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        return 0;
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        return 0;
    }

    @Override
    public int writeTupleFields(ITupleReference tuple, int startField, int numFields, byte[] targetBuf, int targetOff) {
        return 0;
    }

    @Override
    public int bytesRequired(ITupleReference tuple, int startField, int numFields) {
        return 0;
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return null;
    }

    @Override
    public int getCopySpaceRequired(ITupleReference tuple) {
        return 0;
    }
}
