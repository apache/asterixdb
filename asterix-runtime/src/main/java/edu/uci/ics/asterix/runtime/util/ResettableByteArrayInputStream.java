/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.asterix.runtime.util;

import java.io.ByteArrayInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ResettableByteArrayInputStream extends ByteArrayInputStream {
    private static final Logger LOGGER = Logger.getLogger(ResettableByteArrayInputStream.class.getName());

    private byte[] data;
    private int position;

    public ResettableByteArrayInputStream(byte[] data) {
        super(data);
    }

    public void setByteArray(byte[] data, int position) {
        this.data = data;
        this.position = position;
    }

    @Override
    public int read() {
        int remaining = data.length - position;
        int value = remaining > 0 ? (data[position++] & 0xff) : -1;
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("read(): value: " + value + " remaining: " + remaining + " position: " + position);
        }
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        int remaining = data.length - position;
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("read(bytes[], int, int): remaining: " + remaining + " offset: " + offset + " length: "
                    + length + " position: " + position);
        }
        if (remaining == 0) {
            return -1;
        }
        int l = Math.min(length, remaining);
        System.arraycopy(data, position, bytes, offset, l);
        position += l;
        return l;
    }
}