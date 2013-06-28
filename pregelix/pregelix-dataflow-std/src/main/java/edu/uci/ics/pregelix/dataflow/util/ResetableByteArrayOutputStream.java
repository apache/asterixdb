/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.pregelix.dataflow.util;

import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ResetableByteArrayOutputStream extends OutputStream {
    private static final Logger LOGGER = Logger.getLogger(ResetableByteArrayOutputStream.class.getName());

    private byte[] data;
    private int position;

    public ResetableByteArrayOutputStream() {
    }

    public void setByteArray(byte[] data, int position) {
        this.data = data;
        this.position = position;
    }

    @Override
    public void write(int b) {
        int remaining = data.length - position;
        if (position + 1 > data.length - 1)
            throw new IndexOutOfBoundsException();
        data[position] = (byte) b;
        position++;
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("write(): value: " + b + " remaining: " + remaining + " position: " + position);
        }
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("write(bytes[], int, int) offset: " + offset + " length: " + length + " position: "
                    + position);
        }
        if (position + length > data.length - 1)
            throw new IndexOutOfBoundsException();
        System.arraycopy(bytes, offset, data, position, length);
        position += length;
    }
}