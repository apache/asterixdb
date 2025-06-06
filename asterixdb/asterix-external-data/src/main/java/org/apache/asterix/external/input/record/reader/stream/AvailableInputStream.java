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
package org.apache.asterix.external.input.record.reader.stream;

import java.io.IOException;
import java.io.InputStream;

public class AvailableInputStream extends InputStream {
    private final InputStream is;

    public AvailableInputStream(InputStream inputstream) {
        is = inputstream;
    }

    public int read() throws IOException {
        return (is.read());
    }

    public int read(byte[] b) throws IOException {
        return (is.read(b));
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return (is.read(b, off, len));
    }

    public void close() throws IOException {
        is.close();
    }

    public int available() throws IOException {
        // Always say that we have 1 more byte in the
        // buffer, even when we don't
        int a = is.available();
        if (a == 0) {
            return (1);
        } else {
            return (a);
        }
    }
}
