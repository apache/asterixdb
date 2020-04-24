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
package org.apache.hyracks.dataflow.common.data.parsers;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class BooleanParserFactory implements IValueParserFactory {

    private static final long serialVersionUID = 1L;

    public static final IValueParserFactory INSTANCE = new BooleanParserFactory();

    private BooleanParserFactory() {
    }

    @Override
    public IValueParser createValueParser() {
        return BooleanParserFactory::parse;
    }

    public static boolean parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
        char ch;
        int i = start;
        int end = start + length;
        while (i < end && ((ch = buffer[i]) == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f')) {
            i++;
        }
        int remainingLength = end - i;
        boolean gotBoolean = false;
        boolean booleanValue = false;
        if (remainingLength >= 4 && ((ch = buffer[i]) == 't' || ch == 'T') && ((ch = buffer[i + 1]) == 'r' || ch == 'R')
                && ((ch = buffer[i + 2]) == 'u' || ch == 'U') && ((ch = buffer[i + 3]) == 'e' || ch == 'E')) {
            gotBoolean = true;
            booleanValue = true;
            i = i + 4;
        } else if (remainingLength >= 5 && ((ch = buffer[i]) == 'f' || ch == 'F')
                && ((ch = buffer[i + 1]) == 'a' || ch == 'A') && ((ch = buffer[i + 2]) == 'l' || ch == 'L')
                && ((ch = buffer[i + 3]) == 's' || ch == 'S') && ((ch = buffer[i + 4]) == 'e' || ch == 'E')) {
            gotBoolean = true;
            booleanValue = false;
            i = i + 5;
        }

        for (; i < end; ++i) {
            ch = buffer[i];
            if (ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r' && ch != '\f') {
                return false;
            }
        }
        if (!gotBoolean) {
            return false;
        }
        try {
            out.writeBoolean(booleanValue);
            return true;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
