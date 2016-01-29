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
package org.apache.asterix.external.input.record.reader.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

public class HDFSTextLineReader {
    private static final int DEFAULT_BUFFER_SIZE = 32 * 1024;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private FSDataInputStream reader;

    private byte[] buffer;
    // the number of bytes of real data in the buffer
    private int bufferLength = 0;
    // the current position in the buffer
    private int bufferPosn = 0;

    private long currentFilePos = 0L;

    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public static final String KEY_BUFFER_SIZE = "io.file.buffer.size";

    /**
     * Create a line reader that reads from the given stream using the
     * default buffer-size (32k).
     *
     * @param in
     *            The input stream
     * @throws IOException
     */
    public HDFSTextLineReader(FSDataInputStream in) throws IOException {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Create a line reader that reads from the given stream using the
     * given buffer-size.
     *
     * @param in
     *            The input stream
     * @param bufferSize
     *            Size of the read buffer
     * @throws IOException
     */
    public HDFSTextLineReader(FSDataInputStream in, int bufferSize) throws IOException {
        this.reader = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        currentFilePos = in.getPos();
    }

    public HDFSTextLineReader() throws IOException {
        this.bufferSize = DEFAULT_BUFFER_SIZE;
        this.buffer = new byte[this.bufferSize];
    }

    /**
     * Create a line reader that reads from the given stream using the <code>io.file.buffer.size</code> specified in the given <code>Configuration</code>.
     *
     * @param in
     *            input stream
     * @param conf
     *            configuration
     * @throws IOException
     */
    public HDFSTextLineReader(FSDataInputStream in, Configuration conf) throws IOException {
        this(in, conf.getInt(KEY_BUFFER_SIZE, DEFAULT_BUFFER_SIZE));
    }

    /**
     * Read one line from the InputStream into the given Text. A line
     * can be terminated by one of the following: '\n' (LF) , '\r' (CR),
     * or '\r\n' (CR+LF). EOF also terminates an otherwise unterminated
     * line.
     *
     * @param str
     *            the object to store the given line (without newline)
     * @param maxLineLength
     *            the maximum number of bytes to store into str;
     *            the rest of the line is silently discarded.
     * @param maxBytesToConsume
     *            the maximum number of bytes to consume
     *            in this call. This is only a hint, because if the line cross
     *            this threshold, we allow it to happen. It can overshoot
     *            potentially by as much as one buffer length.
     * @return the number of bytes read including the (longest) newline
     *         found.
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        /* We're reading data from in, but the head of the stream may be
         * already buffered in buffer, so we have several cases:
         * 1. No newline characters are in the buffer, so we need to copy
         *    everything and read another buffer from the stream.
         * 2. An unambiguously terminated line is in buffer, so we just
         *    copy to str.
         * 3. Ambiguously terminated line is in buffer, i.e. buffer ends
         *    in CR.  In this case we copy everything up to CR to str, but
         *    we also need to see what follows CR: if it's LF, then we
         *    need consume LF as well, so next call to readLine will read
         *    from after that.
         * We use a flag prevCharCR to signal if previous character was CR
         * and, if it happens to be at the end of the buffer, delay
         * consuming it until we have a chance to look at the char that
         * follows.
         */
        str.clear();
        int txtLength = 0; //tracks str.getLength(), as an optimization
        int newlineLength = 0; //length of terminating newline
        boolean prevCharCR = false; //true of prev char was CR
        long bytesConsumed = 0;
        do {
            int startPosn = bufferPosn; //starting from where we left off the last time
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                if (prevCharCR)
                    ++bytesConsumed; //account for CR from previous read
                bufferLength = reader.read(buffer);
                if (bufferLength <= 0)
                    break; // EOF
            }
            for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
                if (buffer[bufferPosn] == LF) {
                    newlineLength = (prevCharCR) ? 2 : 1;
                    ++bufferPosn; // at next invocation proceed from following byte
                    break;
                }
                if (prevCharCR) { //CR + notLF, we are at notLF
                    newlineLength = 1;
                    break;
                }
                prevCharCR = (buffer[bufferPosn] == CR);
            }
            int readLength = bufferPosn - startPosn;
            if (prevCharCR && newlineLength == 0)
                --readLength; //CR at the end of the buffer
            bytesConsumed += readLength;
            int appendLength = readLength - newlineLength;
            if (appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }
            if (appendLength > 0) {
                str.append(buffer, startPosn, appendLength);
                txtLength += appendLength;
            }
        } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

        if (bytesConsumed > Integer.MAX_VALUE)
            throw new IOException("Too many bytes before newline: " + bytesConsumed);
        currentFilePos = reader.getPos() - bufferLength + bufferPosn;
        return (int) bytesConsumed;
    }

    /**
     * Read from the InputStream into the given Text.
     *
     * @param str
     *            the object to store the given line
     * @param maxLineLength
     *            the maximum number of bytes to store into str.
     * @return the number of bytes read including the newline
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str, int maxLineLength) throws IOException {
        return readLine(str, maxLineLength, Integer.MAX_VALUE);
    }

    /**
     * Read from the InputStream into the given Text.
     *
     * @param str
     *            the object to store the given line
     * @return the number of bytes read including the newline
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str) throws IOException {
        return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public void seek(long desired) throws IOException {
        if (reader.getPos() <= desired || currentFilePos > desired) {
            // desired position is ahead of stream or before the current position, seek to position
            reader.seek(desired);
            bufferLength = 0;
            bufferPosn = 0;
            currentFilePos = desired;
        } else if (currentFilePos < desired) {
            // desired position is in the buffer
            int difference = (int) (desired - currentFilePos);
            bufferPosn += difference;
            currentFilePos = desired;
        }
    }

    public FSDataInputStream getReader() {
        return reader;
    }

    public void resetReader(FSDataInputStream reader) throws IOException {
        this.reader = reader;
        bufferLength = 0;
        bufferPosn = 0;
        currentFilePos = reader.getPos();
    }

    public void close() throws IOException {
        reader.close();
    }
}
