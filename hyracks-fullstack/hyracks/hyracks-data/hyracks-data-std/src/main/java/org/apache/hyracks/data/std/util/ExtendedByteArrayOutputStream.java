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

import java.io.ByteArrayOutputStream;

/**
 * This is an extended class of ByteArrayOutputStream class that can return the current buffer array and its length.
 * Use this class to avoid a new byte[] creation when using toArray() method.
 */
public class ExtendedByteArrayOutputStream extends ByteArrayOutputStream {

    public ExtendedByteArrayOutputStream() {
        super();
    }

    public ExtendedByteArrayOutputStream(int size) {
        super(size);
    }

    public synchronized byte[] getByteArray() {
        return buf;
    }

    /**
     * Returns the current length of this stream (not capacity).
     */
    public synchronized int getLength() {
        return count;
    }
}
