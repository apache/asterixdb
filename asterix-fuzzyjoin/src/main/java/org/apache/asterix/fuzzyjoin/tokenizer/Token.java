/**
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

package org.apache.asterix.fuzzyjoin.tokenizer;

import java.io.Serializable;

public class Token implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private CharSequence data;
    private int start;
    private int length;
    private int count;

    /** Cache the hash code for the string */
    private int hash; // Default to 0

    public Token() {
    }

    public Token(CharSequence data, int start, int length, int count) {
        set(data, start, length, count);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Token)) {
            return false;
        }
        Token t = (Token) o;
        if (t.length != length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (t.data.charAt(t.start + i) != data.charAt(start + i)) {
                return false;
            }
        }
        return true;
    }

    public CharSequence getCharSequence() {
        return data;
    }

    public int getCount() {
        return count;
    }

    public int getLength() {
        return length;
    }

    public int getStart() {
        return start;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && length > 0) {
            for (int i = 0; i < length; i++) {
                h = 31 * h + data.charAt(start + i);
            }
            h = 31 * h + count;
            hash = h;
        }
        return h;
    }

    public int length() {
        return length;
    }

    public void set(CharSequence data, int start, int length, int count) {
        this.data = data;
        this.start = start;
        this.length = length;
        this.count = count;
        hash = 0;
    }

    public void set(String data, int count) {
        this.data = data;
        start = 0;
        length = data.length();
        this.count = count;
        hash = 0;
    }

    @Override
    public String toString() {
        return "(" + data.subSequence(start, start + length) + ", " + count + ")";
    }
}
