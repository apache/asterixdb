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
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class FileLexerSource extends LexerSource {
    // This source allows input from a file
    private BufferedReader reader;
    private boolean unread;
    private boolean finished;
    private boolean nextRead;
    private char nextChar;

    public FileLexerSource(File file) throws IOException {
        this.reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8);
    }

    public void setNewSource(File file) throws IOException {
        if (this.reader != null) {
            reader.close();
        }
        this.reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8);
    }

    @Override
    public char readCharacter() throws IOException {
        if (unread) {
            unread = false;
            return previousCharacter;
        } else if (nextRead) {
            nextRead = false;
            return nextChar;
        }
        previousCharacter = (char) reader.read();
        return previousCharacter;
    }

    @Override
    public void unreadCharacter() throws IOException {
        if (nextRead) {
            throw new IOException("Unexpected Situation");
        } else if (unread) {
            throw new IOException("This lexer source supports only one step back");
        }
        unread = true;
    }

    @Override
    public boolean atEnd() throws IOException {
        if (finished) {
            return true;
        } else if (nextRead) {
            return false;
        }
        nextChar = (char) reader.read();
        if (nextChar < 0) {
            finished = true;
        } else {
            nextRead = true;
        }
        return finished;
    }

    @Override
    public char[] getBuffer() {
        return null;
    }
}
