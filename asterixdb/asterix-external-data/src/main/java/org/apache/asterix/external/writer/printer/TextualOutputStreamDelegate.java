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
package org.apache.asterix.external.writer.printer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * The purpose of this is to catch and check an error that could be thrown by {@link #stream} when printing results.
 * {@link java.io.PrintStream} doesn't throw any exception on write. Hence, we can miss all errors thrown during
 * write operations.
 *
 * @see TextualExternalFilePrinter#print(IValueReference)
 */
final class TextualOutputStreamDelegate extends OutputStream {
    private final OutputStream stream;
    private Exception exception;

    TextualOutputStreamDelegate(OutputStream stream) {
        this.stream = Objects.requireNonNull(stream);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        try {
            stream.write(b, off, len);
        } catch (Exception e) {
            this.exception = e;
            throw e;
        }

    }

    @Override
    public void write(byte[] b) throws IOException {
        try {
            stream.write(b);
        } catch (Exception e) {
            this.exception = e;
            throw e;
        }
    }

    @Override
    public void write(int b) throws IOException {
        try {
            stream.write(b);
        } catch (Exception e) {
            this.exception = e;
            throw e;
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        try {
            stream.flush();
        } catch (Exception e) {
            this.exception = e;
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            stream.close();
        } catch (Exception e) {
            this.exception = e;
            throw e;
        }
    }

    void checkError() throws HyracksDataException {
        if (exception != null) {
            throw HyracksDataException.create(exception);
        }
    }
}
