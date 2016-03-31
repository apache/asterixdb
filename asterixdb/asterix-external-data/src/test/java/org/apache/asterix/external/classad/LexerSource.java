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

public abstract class LexerSource {
    /*
     * LexerSource is a class that provides an abstract interface to the
     * lexer. The lexer reads tokens and gives them to the parser.  The
     * lexer reads single characters from a LexerSource. Because we want
     * to read characters from different types of sources (files, strings,
     * etc.) without burdening the lexer, the lexer uses this LexerSource
     * abstract class. There are several implementations of the
     * LexerSource that provide access to specific types of sources.
     *
     */
    protected char previousCharacter;

    public int getPosition() {
        return -1;
    }

    // Reads a single character from the source
    public abstract char readCharacter() throws IOException;

    // Returns the last character read (from ReadCharacter()) from the
    // source
    public char readPreviousCharacter() {
        return previousCharacter;
    }

    // Puts back a character so that when ReadCharacter is called
    // again, it returns the character that was previously
    // read. Although this interface appears to require the ability to
    // put back an arbitrary number of characters, in practice we only
    // ever put back a single character.
    public abstract void unreadCharacter() throws IOException;

    public abstract boolean atEnd() throws IOException;

    public abstract char[] getBuffer();

    public void setNewSource(char[] recordCharBuffer) {
    }
}
