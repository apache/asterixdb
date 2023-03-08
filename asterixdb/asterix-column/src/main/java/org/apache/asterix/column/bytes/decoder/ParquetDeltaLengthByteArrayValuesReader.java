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
package org.apache.asterix.column.bytes.decoder;

import java.io.IOException;

import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.io.ParquetDecodingException;

public class ParquetDeltaLengthByteArrayValuesReader extends AbstractParquetValuesReader {

    private final VoidPointable value;
    private final AbstractParquetValuesReader lengthReader;
    private AbstractBytesInputStream in;

    public ParquetDeltaLengthByteArrayValuesReader() {
        this.lengthReader = new ParquetDeltaBinaryPackingValuesReader();
        value = new VoidPointable();
    }

    @Override
    public void initFromPage(AbstractBytesInputStream stream) throws IOException {
        AbstractBytesInputStream lengthStream = stream.sliceStream(BytesUtils.readUnsignedVarInt(stream));
        lengthReader.initFromPage(lengthStream);
        this.in = stream;
    }

    @Override
    public void skip() {
        int length = lengthReader.readInteger();
        try {
            in.skipFully(length);
        } catch (IOException e) {
            throw new ParquetDecodingException("Failed to skip " + length + " bytes");
        }
    }

    @Override
    public IValueReference readBytes() {
        int length = lengthReader.readInteger();
        try {
            in.read(value, length);
            return value;
        } catch (IOException e) {
            throw new ParquetDecodingException("Failed to read " + length + " bytes");
        }
    }

}
