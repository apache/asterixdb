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
package org.apache.asterix.external.classad;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class InputStreamLexerSource extends LexerSource {
    // This source allows input from a stream. Note that
    // the user passes in a pointer to the stream.

    public int position = 0;
    public char[] buffer = new char[512];
    private BufferedReader reader;
    public int validBytes;

    public InputStreamLexerSource(InputStream in) {
        this.reader = new BufferedReader(new InputStreamReader(in));
    }

    @Override
    public int getPosition() {
        return position;
    }

    public InputStreamLexerSource() {
    }

    public InputStreamLexerSource(BufferedReader reader) {
        this.reader = reader;
    }

    @Override
    public char readCharacter() throws IOException {
        if (position < validBytes) {
            previousCharacter = buffer[position];
            position++;
            return previousCharacter;
        } else {
            fillBuffer();
        }
        if (position < validBytes) {
            previousCharacter = buffer[position];
            position++;
            return previousCharacter;
        }
        return '\0';
    }

    private void fillBuffer() throws IOException {
        position = 0;
        // we leave an empty location at the end to take care of corner case of unread
        validBytes = reader.read(buffer, 0, buffer.length - 1);
    }

    @Override
    public void unreadCharacter() {
        if (position == 0) {
            System.arraycopy(buffer, 0, buffer, 1, buffer.length - 1);
            buffer[0] = previousCharacter;
            validBytes++;
            return;
        } else {
            position--;
        }
    }

    @Override
    public boolean atEnd() throws IOException {
        if (position < validBytes) {
            return false;
        }
        fillBuffer();
        return position == validBytes;
    }

    public void setNewSource(InputStream stream) {
        this.reader = new BufferedReader(new InputStreamReader(stream));
        this.position = 0;
        this.validBytes = 0;
    }

    public void setNewSource(BufferedReader reader) {
        this.reader = reader;
        this.position = 0;
        this.validBytes = 0;
    }

    @Override
    public char[] getBuffer() {
        return buffer;
    }
}
