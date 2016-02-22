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

public class StringLexerSource extends LexerSource {
    private String aString;
    private int offset;

    public StringLexerSource(String aString, int offset) {
        setNewSource(aString, offset);
    }

    public StringLexerSource(String aString) {
        setNewSource(aString, 0);
    }

    @Override
    public int getPosition() {
        return offset;
    }

    public void setNewSource(String aString, int offset) {
        this.aString = aString;
        this.offset = offset;
    }

    @Override
    public char readCharacter() {
        if (offset == aString.length()) {
            previousCharacter = Lexer.EOF;
            return previousCharacter;
        } else {
            previousCharacter = aString.charAt(offset);
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
        return offset == aString.length();
    }

    public int getCurrentLocation() {
        return offset;
    }

    public void setNewSource(String buffer) {
        setNewSource(buffer, 0);
    }

    @Override
    public char[] getBuffer() {
        return aString.toCharArray();
    }

}
