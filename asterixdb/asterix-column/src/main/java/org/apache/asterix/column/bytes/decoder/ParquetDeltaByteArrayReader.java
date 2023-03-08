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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Re-implementation of {@link DeltaByteArrayReader}
 */
public class ParquetDeltaByteArrayReader extends AbstractParquetValuesReader {
    private final AbstractParquetValuesReader prefixLengthReader;
    private final ParquetDeltaLengthByteArrayValuesReader suffixReader;
    private final byte[] lengthBytes;

    private final ArrayBackedValueStorage temp;
    private final ArrayBackedValueStorage previous;
    boolean newPage;

    public ParquetDeltaByteArrayReader(boolean containsLength) {
        this.prefixLengthReader = new ParquetDeltaBinaryPackingValuesReader();
        this.suffixReader = new ParquetDeltaLengthByteArrayValuesReader();
        this.temp = new ArrayBackedValueStorage();
        this.previous = new ArrayBackedValueStorage();
        lengthBytes = containsLength ? new byte[4] : new byte[0];
    }

    @Override
    public void initFromPage(AbstractBytesInputStream stream) throws IOException {
        AbstractBytesInputStream prefixStream = stream.sliceStream(BytesUtils.readUnsignedVarInt(stream));
        prefixLengthReader.initFromPage(prefixStream);
        suffixReader.initFromPage(stream);
        previous.reset();
        temp.reset();
        newPage = true;
    }

    @Override
    public void skip() {
        // read the next value to skip so that previous is correct.
        this.readBytes();
    }

    @Override
    public IValueReference readBytes() {
        int prefixLength = prefixLengthReader.readInteger();
        // This does not copy bytes
        IValueReference suffix = suffixReader.readBytes();

        // NOTE: due to PARQUET-246, it is important that we
        // respect prefixLength which was read from prefixLengthReader,
        // even for the *first* value of a page. Even though the first
        // value of the page should have an empty prefix, it may not
        // because of PARQUET-246.

        // We have to do this to materialize the output
        try {
            int lengthSize;
            if (prefixLength != 0) {
                lengthSize = appendLength(prefixLength + suffix.getLength());
                temp.append(previous.getByteArray(), previous.getStartOffset(), prefixLength);
            } else {
                lengthSize = appendLength(suffix.getLength());
            }
            temp.append(suffix);
            /*
             * Adding length after appending prefix and suffix is important as we do not overwrite the original
             * previous bytes
             * */
            System.arraycopy(lengthBytes, 0, temp.getByteArray(), 0, lengthSize);
            previous.set(temp.getByteArray(), temp.getStartOffset() + lengthSize, temp.getLength() - lengthSize);
        } catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
        newPage = false;
        return temp;
    }

    private int appendLength(int length) {
        if (lengthBytes.length > 0) {
            int numOfBytes = UTF8StringUtil.encodeUTF8Length(length, lengthBytes, 0);
            temp.setSize(numOfBytes);
            return numOfBytes;
        }
        temp.setSize(0);
        return 0;
    }

}
