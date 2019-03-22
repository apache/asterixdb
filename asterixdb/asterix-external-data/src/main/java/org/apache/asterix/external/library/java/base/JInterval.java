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
package org.apache.asterix.external.library.java.base;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class JInterval extends JObject {

    public JInterval(long intervalStart, long intervalEnd) {
        super(new AMutableInterval(intervalStart, intervalEnd, (byte) 0));
    }

    public void setValue(long intervalStart, long intervalEnd, byte typetag) throws HyracksDataException {
        ((AMutableInterval) value).setValue(intervalStart, intervalEnd, typetag);
    }

    public long getIntervalStart() {
        return ((AMutableInterval) value).getIntervalStart();
    }

    public long getIntervalEnd() {
        return ((AMutableInterval) value).getIntervalEnd();
    }

    public short getIntervalType() {
        return ((AMutableInterval) value).getIntervalType();
    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        serializeTypeTag(writeTypeTag, dataOutput, ATypeTag.INTERVAL);
        AIntervalSerializerDeserializer.INSTANCE.serialize((AMutableInterval) value, dataOutput);
    }

    @Override
    public void reset() throws HyracksDataException {
        ((AMutableInterval) value).setValue(0L, 0L, (byte) 0);
    }

    @Override
    public IAType getIAType() {
        return BuiltinType.AINTERVAL;
    }
}
