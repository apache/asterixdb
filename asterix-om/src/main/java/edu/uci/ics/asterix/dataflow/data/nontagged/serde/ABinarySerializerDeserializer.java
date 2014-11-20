/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import edu.uci.ics.asterix.om.base.ABinary;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.ByteArrayPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ABinarySerializerDeserializer implements ISerializerDeserializer<ABinary>{

    private static final long serialVersionUID = 1L;
    public static final int SIZE_OF_LENGTH = ByteArrayPointable.SIZE_OF_LENGTH;

    public static final ABinarySerializerDeserializer INSTANCE = new ABinarySerializerDeserializer();

    private ABinarySerializerDeserializer(){}

    @Override
    public ABinary deserialize(DataInput in) throws HyracksDataException {
        return new ABinary(ByteArraySerializerDeserializer.INSTANCE.deserialize(in));
    }

    @Override
    public void serialize(ABinary binary, DataOutput out) throws HyracksDataException {
        ByteArraySerializerDeserializer.INSTANCE.serialize(binary.getBytes(), binary.getStart(),
                binary.getLength() , out);
    }

    public static int getLength(byte [] bytes, int offset){
        return ByteArrayPointable.getLength(bytes,offset);
    }

    public static void putLength(int length, byte [] bytes, int offset){
        ByteArrayPointable.putLength(length, bytes, offset);
    }
}
