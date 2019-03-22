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

import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.om.base.AMutableTime;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class JTime extends JObject {

    public JTime(int timeInMillsec) {
        super(new AMutableTime(timeInMillsec));
    }

    public void setValue(int timeInMillsec) {
        ((AMutableTime) value).setValue(timeInMillsec);
    }

    public int getValue() {
        return ((AMutableTime) value).getChrononTime();
    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        serializeTypeTag(writeTypeTag, dataOutput, ATypeTag.TIME);
        ATimeSerializerDeserializer.INSTANCE.serialize((AMutableTime) value, dataOutput);
    }

    @Override
    public void reset() {
        ((AMutableTime) value).setValue(0);
    }

    @Override
    public IAType getIAType() {
        return BuiltinType.ATIME;
    }
}
