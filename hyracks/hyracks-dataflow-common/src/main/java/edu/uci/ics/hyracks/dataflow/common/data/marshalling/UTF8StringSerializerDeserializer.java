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
package edu.uci.ics.hyracks.dataflow.common.data.marshalling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class UTF8StringSerializerDeserializer implements ISerializerDeserializer<String> {
    public static final UTF8StringSerializerDeserializer INSTANCE = new UTF8StringSerializerDeserializer();

    private static final long serialVersionUID = 1L;

    private UTF8StringSerializerDeserializer() {
    }

    @Override
    public String deserialize(DataInput in) throws HyracksDataException {
        try {
            return in.readUTF();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(String instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeUTF(instance);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}