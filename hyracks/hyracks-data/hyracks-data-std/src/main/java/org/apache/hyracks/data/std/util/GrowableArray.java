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
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.api.IValueReference;

public class GrowableArray implements IDataOutputProvider {
    private final ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
    private final DataOutputStream dos = new DataOutputStream(baaos);

    @Override
    public DataOutput getDataOutput() {
        return dos;
    }

    public void reset() {
        baaos.reset();
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
