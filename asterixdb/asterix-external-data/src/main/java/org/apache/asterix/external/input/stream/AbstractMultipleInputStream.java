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
import java.io.InputStream;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IStreamNotificationHandler;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Base class for a source stream that is composed of multiple separate input streams. Reading proceeds one stream at
 * a time.
 */
public abstract class AbstractMultipleInputStream extends AsterixInputStream {

    protected InputStream in;
    private byte lastByte;

    protected AbstractMultipleInputStream() {
    }

    /**
     * Closes the current input stream and opens the next one, if any. Implementations should call
     * {@link IStreamNotificationHandler#notifyNewSource()} using {@link #notificationHandler} if there exists a
     * notification handler and the handler needs to know when a new input stream has started. Obviously, this method
     * should populate the {@link #in} upon successfully opening the stream.
     */
    protected abstract boolean advance() throws IOException;

    @Override
    public int read() throws IOException {
        throw new HyracksDataException(
                "read() is not supported with this stream. use read(byte[] b, int off, int len)");
    }

    @Override
    public final int read(byte[] b, int off, int len) throws IOException {
        if (in == null) {
            if (!advance()) {
                return -1;
            }
        }
        int result = in.read(b, off, len);
        if (result < 0 && (lastByte != ExternalDataConstants.BYTE_LF) && (lastByte != ExternalDataConstants.BYTE_CR)) {
            // return a new line at the end of every file <--Might create problems for some cases
            // depending on the parser implementation-->
            lastByte = ExternalDataConstants.BYTE_LF;
            b[off] = ExternalDataConstants.BYTE_LF;
            return 1;
        }
        while ((result < 0) && advance()) {
            result = in.read(b, off, len);
        }
        if (result > 0) {
            lastByte = b[(off + result) - 1];
        }
        return result;
    }
}
