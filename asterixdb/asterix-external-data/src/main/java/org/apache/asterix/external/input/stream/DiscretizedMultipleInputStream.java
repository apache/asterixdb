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
package org.apache.asterix.external.input.stream;

import java.io.IOException;

import org.apache.asterix.external.api.AsterixInputStream;

public class DiscretizedMultipleInputStream extends AsterixInputStream {
    private final IStreamWrapper stream;

    public DiscretizedMultipleInputStream(AsterixInputStream inputStream) {
        if (inputStream instanceof AbstractMultipleInputStream) {
            AbstractMultipleInputStream multipleInputStream = (AbstractMultipleInputStream) inputStream;
            stream = new MultipleStreamWrapper(multipleInputStream);
        } else {
            stream = new SingleStreamWrapper(inputStream);
        }
    }

    @Override
    public int read() throws IOException {
        return stream.read();
    }

    public boolean advance() throws IOException {
        return stream.advance();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return stream.read(b, off, len);
    }

    @Override
    public boolean stop() throws Exception {
        return stream.getInputStream().stop();
    }

    @Override
    public boolean handleException(Throwable th) {
        return stream.getInputStream().handleException(th);
    }

    private interface IStreamWrapper {
        boolean advance() throws IOException;

        int read() throws IOException;

        int read(byte[] b, int off, int len) throws IOException;

        AsterixInputStream getInputStream();
    }

    private static class MultipleStreamWrapper implements IStreamWrapper {
        private final AbstractMultipleInputStream inputStream;

        private MultipleStreamWrapper(AbstractMultipleInputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public boolean advance() throws IOException {
            return inputStream.advance();
        }

        @Override
        public int read() throws IOException {
            return inputStream.in.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return inputStream.in.read(b, off, len);
        }

        @Override
        public AsterixInputStream getInputStream() {
            return inputStream;
        }
    }

    private static class SingleStreamWrapper implements IStreamWrapper {
        private final AsterixInputStream inputStream;

        private SingleStreamWrapper(AsterixInputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public boolean advance() {
            return false;
        }

        @Override
        public int read() throws IOException {
            return inputStream.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return inputStream.read(b, off, len);
        }

        @Override
        public AsterixInputStream getInputStream() {
            return inputStream;
        }
    }

}
