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

public class LongParserFactory implements IValueParserFactory {
    public static final IValueParserFactory INSTANCE = new LongParserFactory();

    private static final long serialVersionUID = 1L;

    private LongParserFactory() {
    }

    @Override
    public IValueParser createValueParser() {
        return new IValueParser() {
            @Override
            public boolean parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                // accumulating negatively like Long.parse() to avoid surprises near MAX_VALUE
                char c;
                int i = start;
                int end = start + length;
                while (i < end && ((c = buffer[i]) == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f')) {
                    i++;
                }
                boolean negative = false;
                long limit = -Long.MAX_VALUE;
                if (i < end) {
                    c = buffer[i];
                    if (c == '-') {
                        negative = true;
                        limit = Long.MIN_VALUE;
                        i++;
                    }
                    if (c == '+') {
                        i++;
                    }
                }
                long result = 0;
                long multiplicationMin = limit / 10;
                boolean gotNumber = false;
                for (; i < end; i++) {
                    c = buffer[i];
                    if (c >= '0' && c <= '9') {
                        gotNumber = true;
                        if (result < multiplicationMin) {
                            return false;
                        }
                        result *= 10;
                        int digit = c - '0';
                        if (result < limit + digit) {
                            return false;
                        }
                        result -= digit;
                    } else {
                        break;
                    }
                }

                for (; i < end; ++i) {
                    c = buffer[i];
                    if (c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '\f') {
                        return false;
                    }
                }

                if (!gotNumber) {
                    return false;
                }
                try {
                    out.writeLong(negative ? result : -result);
                    return true;
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
        };
    }
}
