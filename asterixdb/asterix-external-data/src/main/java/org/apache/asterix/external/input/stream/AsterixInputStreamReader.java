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
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AsterixInputStreamReader extends Reader {
    private AsterixInputStream in;
    private byte[] bytes = new byte[ExternalDataConstants.DEFAULT_BUFFER_SIZE];
    private ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    private CharBuffer charBuffer = CharBuffer.allocate(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
    private CharsetDecoder decoder;
    private boolean done = false;
    private boolean remaining = false;

    public AsterixInputStreamReader(AsterixInputStream in) {
        this.in = in;
        this.decoder = StandardCharsets.UTF_8.newDecoder();
        this.byteBuffer.flip();
    }

    public void stop() throws IOException {
        try {
            in.stop();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void setController(AbstractFeedDataFlowController controller) {
        in.setController(controller);
    }

    public void setFeedLogManager(FeedLogManager feedLogManager) throws HyracksDataException {
        in.setFeedLogManager(feedLogManager);
    }

    @Override
    public int read(char cbuf[]) throws IOException {
        return read(cbuf, 0, cbuf.length);
    }

    @Override
    public int read(char cbuf[], int offset, int length) throws IOException {
        if (done) {
            return -1;
        }
        int len = 0;
        charBuffer.clear();
        while (charBuffer.position() == 0) {
            if (byteBuffer.hasRemaining()) {
                remaining = true;
                decoder.decode(byteBuffer, charBuffer, false);
                System.arraycopy(charBuffer.array(), 0, cbuf, offset, charBuffer.position());
                if (charBuffer.position() > 0) {
                    return charBuffer.position();
                } else {
                    // need to read more data
                    System.arraycopy(bytes, byteBuffer.position(), bytes, 0, byteBuffer.remaining());
                    byteBuffer.position(byteBuffer.remaining());
                    while (len == 0) {
                        len = in.read(bytes, byteBuffer.position(), bytes.length - byteBuffer.position());
                    }
                }
            } else {
                byteBuffer.clear();
                while (len == 0) {
                    len = in.read(bytes, 0, bytes.length);
                }
            }
            if (len == -1) {
                done = true;
                return len;
            }
            if (remaining) {
                byteBuffer.position(len + byteBuffer.position());
            } else {
                byteBuffer.position(len);
            }
            byteBuffer.flip();
            remaining = false;
            decoder.decode(byteBuffer, charBuffer, false);
            System.arraycopy(charBuffer.array(), 0, cbuf, offset, charBuffer.position());
        }
        return charBuffer.position();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    public boolean handleException(Throwable th) {
        return in.handleException(th);
    }

    @Override
    public void reset() throws IOException {
        byteBuffer.limit(0);
    }
}
