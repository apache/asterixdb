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

import java.io.IOException;

public class CharArrayLexerSource extends LexerSource {

    private char[] input;
    private int offset;

    @Override
    public int getPosition() {
        return offset;
    }

    public CharArrayLexerSource() {
        offset = 0;
        input = null;
    }

    public CharArrayLexerSource(char[] input, int offset) {
        SetNewSource(input, offset);
    }

    private void SetNewSource(char[] input, int offset2) {
        this.input = input;
        this.offset = offset2;
    }

    @Override
    public char readCharacter() throws IOException {
        if (offset == input.length) {
            previousCharacter = Lexer.EOF;
            return previousCharacter;
        } else {
            previousCharacter = input[offset];
            offset++;
            return previousCharacter;
        }
    }

    @Override
    public void unreadCharacter() {
        if (offset > 0) {
            if (previousCharacter != Lexer.EOF) {
                offset--;
            }
        }
    }

    @Override
    public boolean atEnd() {
        return offset == input.length;
    }

    public int GetCurrentLocation() {
        return offset;
    }

    @Override
    public void setNewSource(char[] buffer) {
        SetNewSource(buffer, 0);
    }

    @Override
    public char[] getBuffer() {
        return input;
    }
}
