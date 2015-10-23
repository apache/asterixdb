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

package org.apache.hyracks.data.std.util;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.api.IValueReference;

public class GrowableArray implements IDataOutputProvider {
    private final ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
    private final RewindableDataOutputStream dos = new RewindableDataOutputStream(baaos);

    @Override
    public DataOutput getDataOutput() {
        return dos;
    }

    public void reset() {
        baaos.reset();
    }

    /**
     * Rewind the current position by {@code delta} to a previous position.
     * This function is used to drop the already written delta bytes.
     * In some cases, we write some bytes, and afterward we found we've written more than expected.
     * Then we need to fix the position by rewind the current position to the expected one.
     *
     * Currently, it is used by the {@link AbstractVarLenObjectBuilder} which may take more space than required
     * at beginning, and it will shift the data and fix the position whenever required.
     * It will throw {@link IndexOutOfBoundsException} if the {@code delta} is negative.
     * Evil function, use with caution.
     *
     * @param delta
     */
    public void rewindPositionBy(int delta) {
        baaos.rewindPositionBy(delta);
        dos.rewindWrittenBy(delta);
    }

    public byte[] getByteArray() {
        return baaos.getByteArray();
    }

    public int getLength() {
        return baaos.size();
    }

    public void append(IValueReference value) throws IOException {
        dos.write(value.getByteArray(), value.getStartOffset(), value.getLength());
    }
}
