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

public class AMutableCharArrayString implements Comparable<AMutableCharArrayString>, CharSequence {
    private char[] value;
    private int length;
    private int increment = 64;

    public AMutableCharArrayString(String str) {
        this.value = str.toCharArray();
        this.length = value.length;
    }

    public void decrementLength() {
        length--;
    }

    public AMutableCharArrayString() {
        length = 0;
        value = new char[increment];
    }

    @Override
    public char charAt(int i) {
        return value[i];
    }

    @Override
    public String toString() {
        return String.valueOf(value, 0, length);
    }

    public AMutableCharArrayString(char[] value, int length) {
        this.value = value;
        this.length = length;
    }

    public AMutableCharArrayString(AMutableCharArrayString aMutableCharArrayString) {
        this.value = new char[aMutableCharArrayString.length];
        setValue(aMutableCharArrayString);
    }

    public AMutableCharArrayString(int iniitialSize) {
        this.value = new char[iniitialSize];
        this.length = 0;
    }

    private void expand() {
        char[] tmpValue = new char[length + increment];
        System.arraycopy(value, 0, tmpValue, 0, length);
        value = tmpValue;
    }

    private void copyAndExpand(int newSize) {
        char[] tmpValue = new char[newSize];
        System.arraycopy(value, 0, tmpValue, 0, length);
        value = tmpValue;
    }

    public void appendChar(char aChar) {
        if (length == value.length) {
            expand();
        }
        value[length] = aChar;
        length++;
    }

    public void erase(int position) {
        if (position != length - 1) {
            System.arraycopy(value, position + 1, value, position, length - (position + 1));
        }
        length--;
    }

    public void setLength(int l) {
        this.length = l;
    }

    public void setValue(AMutableCharArrayString otherString) {
        if (otherString.length > value.length) {
            // need to reallocate
            value = new char[otherString.length];
        }
        System.arraycopy(otherString.value, 0, value, 0, otherString.length);
        this.length = otherString.length;
    }

    public void setValue(String format) {
        reset();
        appendString(format);
    }

    public void reset() {
        this.length = 0;
    }

    public void setValue(AMutableCharArrayString otherString, int length) {
        if (length > value.length) {
            // need to reallocate
            value = new char[length];
        }
        System.arraycopy(otherString.value, 0, value, 0, length);
        this.length = length;
    }

    public void setValue(String otherString, int length) {
        if (length > value.length) {
            // need to reallocate
            value = new char[length];
        }
        otherString.getChars(0, otherString.length(), value, 0);
        this.length = length;
    }

    public void copyValue(char[] value, int length) {
        if (length > this.value.length) {
            // need to reallocate
            this.value = new char[length];
        }
        System.arraycopy(value, 0, this.value, 0, length);
        this.length = length;
    }

    public void setString(char[] value, int length) {
        this.value = value;
        this.length = length;
    }

    public void setChar(int i, char ch) {
        value[i] = ch;
    }

    public void incrementLength() {
        if (value.length == length) {
            expand();
        }
        length++;
    }

    public boolean isEqualsIgnoreCaseLower(char[] compareTo) {
        if (length == compareTo.length) {
            for (int i = 0; i < length; i++) {
                if (compareTo[i] != Character.toLowerCase(value[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public void appendString(String aString) {
        if (value.length - length < aString.length()) {
            copyAndExpand(value.length + aString.length());
        }
        aString.getChars(0, aString.length(), value, length);
        length += aString.length();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AMutableCharArrayString) {
            AMutableCharArrayString s = (AMutableCharArrayString) o;
            if (length == s.length) {
                for (int i = 0; i < length; i++) {
                    if (value[i] != s.value[i]) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public boolean equalsIgnoreCase(Object o) {
        if (o instanceof AMutableCharArrayString) {
            AMutableCharArrayString s = (AMutableCharArrayString) o;
            if (length == s.length) {
                for (int i = 0; i < length; i++) {
                    if (Character.toLowerCase(value[i]) != Character.toLowerCase(s.value[i])) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public boolean equalsString(String aString) {
        if (length == aString.length()) {
            for (int i = 0; i < length; i++) {
                if (value[i] != aString.charAt(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public void erase(int position, int length) {
        if (length + position >= this.length) {
            this.length -= length;
        } else {
            System.arraycopy(value, position + length, value, position, this.length - (position + length));
            this.length -= length;
        }
    }

    public String substr(int i, int len) {
        return String.copyValueOf(value, i, len);
    }

    public int firstNonDigitChar() {
        for (int i = 0; i < length; i++) {
            if (!Character.isDigit(value[i])) {
                return i;
            }
        }
        return -1;
    }

    public int fistNonDoubleDigitChar() {
        boolean inFraction = false;
        boolean prevCharIsPoint = false;
        for (int i = 0; i < length; i++) {
            if (!Character.isDigit(value[i])) {
                if (inFraction) {
                    if (prevCharIsPoint) {
                        return i - 1;
                    } else {
                        return i;
                    }
                } else {
                    if (value[i] == '.') {
                        inFraction = true;
                        prevCharIsPoint = true;
                    }
                }
            } else {
                prevCharIsPoint = false;
            }
        }
        return -1;
    }

    @Override
    public int compareTo(AMutableCharArrayString o) {
        return toString().compareTo(o.toString());
    }

    public int compareTo(String o) {
        return toString().compareTo(o);
    }

    public int compareToIgnoreCase(AMutableCharArrayString o) {
        return toString().compareToIgnoreCase(o.toString());
    }

    public void appendString(AMutableCharArrayString aString) {
        if (value.length - length < aString.length()) {
            copyAndExpand(value.length + aString.length());
        }
        System.arraycopy(aString.value, 0, value, length, aString.length);
        length += aString.length();
    }

    @Override
    public int length() {
        return length;
    }

    public int size() {
        return length;
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return substr(start, end - start);
    }

    public int firstIndexOf(char delim) {
        return firstIndexOf(delim, 0);
    }

    public int firstIndexOf(char delim, int startIndex) {
        int position = startIndex;
        while (position < length) {
            if (value[position] == delim) {
                return position;
            }
            position++;
        }
        return -1;
    }

    public String substr(int lastIndex) {
        return String.copyValueOf(value, lastIndex, length - lastIndex);
    }

    public void prependChar(char c) {
        if (value.length == length) {
            copyAndExpand(value.length * 2);
        }
        System.arraycopy(value, 0, value, 1, length);
        value[0] = c;
        length += 1;
    }

    public void insert(int i, String aString) {
        if (value.length - length < aString.length()) {
            copyAndExpand(value.length + aString.length());
        }
        System.arraycopy(value, i, value, i + aString.length(), aString.length());
        aString.getChars(0, aString.length(), value, i);
        length += aString.length();
    }

    public char[] getValue() {
        return value;
    }

    public int getLength() {
        return length;
    }

    public int getIncrement() {
        return increment;
    }

    public void setValue(char[] value) {
        this.value = value;
    }

    public void setIncrement(int increment) {
        this.increment = increment;
    }
}
