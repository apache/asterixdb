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
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.base.AYearMonthDuration;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AYearMonthDurationSerializerDeserializer implements ISerializerDeserializer<AYearMonthDuration> {

    private static final long serialVersionUID = 1L;

    public static final AYearMonthDurationSerializerDeserializer INSTANCE = new AYearMonthDurationSerializerDeserializer();

    private AYearMonthDurationSerializerDeserializer() {
    }

    @Override
    public AYearMonthDuration deserialize(DataInput in) throws HyracksDataException {
        try {
            return new AYearMonthDuration(in.readInt());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(AYearMonthDuration instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.getMonths());
        } catch (IOException e) {
            throw new HyracksDataException();
        }
    }

    public static int getYearMonth(byte[] data, int offset) {
        return AInt32SerializerDeserializer.getInt(data, offset);
    }

}
