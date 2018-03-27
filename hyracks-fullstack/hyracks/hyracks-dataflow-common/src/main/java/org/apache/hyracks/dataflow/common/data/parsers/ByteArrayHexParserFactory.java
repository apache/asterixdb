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

package org.apache.hyracks.dataflow.common.data.parsers;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.util.bytes.HexParser;

public class ByteArrayHexParserFactory implements IValueParserFactory {
    private static final long serialVersionUID = 1L;
    public static ByteArrayHexParserFactory INSTANCE = new ByteArrayHexParserFactory();

    private ByteArrayHexParserFactory() {
    }

    @Override
    public IValueParser createValueParser() {
        return new IValueParser() {
            HexParser parser = new HexParser();
            ByteArraySerializerDeserializer serializer = ByteArraySerializerDeserializer.INSTANCE;

            @Override
            public void parse(char[] input, int start, int length, DataOutput out) throws HyracksDataException {
                try {
                    parser.generateByteArrayFromHexString(input, start, length);
                    serializer.serialize(parser.getByteArray(), 0, parser.getLength(), out);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
        };
    }

}
