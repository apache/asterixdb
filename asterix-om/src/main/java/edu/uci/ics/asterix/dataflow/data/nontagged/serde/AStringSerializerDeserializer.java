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
package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class AStringSerializerDeserializer implements ISerializerDeserializer<AString> {

    private static final long serialVersionUID = 1L;

    public static final AStringSerializerDeserializer INSTANCE = new AStringSerializerDeserializer();

    private AStringSerializerDeserializer() {
    }

    @Override
    public AString deserialize(DataInput in) throws HyracksDataException {
        try {
            return new AString(UTF8StringSerializerDeserializer.INSTANCE.deserialize(in));
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(AString instance, DataOutput out) throws HyracksDataException {
        try {
            UTF8StringSerializerDeserializer.INSTANCE.serialize(instance.getStringValue(), out);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}
