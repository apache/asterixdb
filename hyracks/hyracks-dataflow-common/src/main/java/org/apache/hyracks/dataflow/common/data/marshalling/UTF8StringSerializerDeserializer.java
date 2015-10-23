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
package org.apache.hyracks.dataflow.common.data.marshalling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class UTF8StringSerializerDeserializer implements ISerializerDeserializer<String> {

    private static final long serialVersionUID = 1L;
    private UTF8StringReader reader = new UTF8StringReader();
    private UTF8StringWriter writer = new UTF8StringWriter();

    public UTF8StringSerializerDeserializer() {}

    @Override
    public String deserialize(DataInput in) throws HyracksDataException {
        try {
            return reader.readUTF(in);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(String instance, DataOutput out) throws HyracksDataException {
        try {
            writer.writeUTF8(instance, out);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}