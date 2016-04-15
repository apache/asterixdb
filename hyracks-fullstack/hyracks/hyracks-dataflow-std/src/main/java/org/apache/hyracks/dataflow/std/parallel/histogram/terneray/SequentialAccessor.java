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

package org.apache.hyracks.dataflow.std.parallel.histogram.terneray;

import org.apache.hyracks.dataflow.std.parallel.ISequentialAccessor;

/**
 * @author michael
 */
public class SequentialAccessor implements ISequentialAccessor {
    boolean initialized = false;
    String string;
    int cursor = 0;

    public static final SequentialAccessor INSTANCE = new SequentialAccessor();

    public ISequentialAccessor create(String s) {
        return new SequentialAccessor(s);
    }

    public SequentialAccessor() {
        cursor = 0;
    }

    public SequentialAccessor(String str) {
        this.string = str;
        cursor = 0;
    }

    @Override
    public char first() {
        if (string.length() > 0) {
            reset();
            return string.charAt(0);
        } else
            return 0;
    }

    @Override
    public char current() {
        if (cursor < string.length())
            return string.charAt(cursor);
        else
            return 0;
    }

    @Override
    public char next() {
        if (++cursor < string.length())
            return string.charAt(cursor);
        else {
            return 0;
        }
    }

    @Override
    public void reset() {
        cursor = 0;
    }

    @Override
    public char at(int i) {
        return string.charAt(i);
    }

    @Override
    public int length() {
        // TODO Auto-generated method stub
        return string.length();
    }

    @Override
    public int cursor() {
        // TODO Auto-generated method stub
        return cursor;
    }

    @Override
    public String toString() {
        return string;
    }
}
