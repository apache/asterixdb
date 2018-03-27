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

public class IntegerParserFactory implements IValueParserFactory {
    public static final IValueParserFactory INSTANCE = new IntegerParserFactory();

    private static final long serialVersionUID = 1L;

    private IntegerParserFactory() {
    }

    @Override
    public IValueParser createValueParser() {
        return new IValueParser() {
            @Override
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                int n = 0;
                int sign = 1;
                int i = 0;
                boolean pre = true;
                for (; pre && i < length; ++i) {
                    char ch = buffer[i + start];
                    switch (ch) {
                        case ' ':
                        case '\t':
                        case '\n':
                        case '\r':
                        case '\f':
                            break;

                        case '-':
                            sign = -1;
                            pre = false;
                            break;

                        case '0':
                        case '1':
                        case '2':
                        case '3':
                        case '4':
                        case '5':
                        case '6':
                        case '7':
                        case '8':
                        case '9':
                            pre = false;
                            n = n * 10 + (ch - '0');
                            break;

                        default:
                            String errorString = new String(buffer, i + start, length - i);
                            throw new HyracksDataException(
                                    "Integer Parser - a digit is expected. But, encountered this character: " + ch
                                            + " in the incoming input: " + errorString);
                    }
                }
                boolean post = false;
                for (; !post && i < length; ++i) {
                    char ch = buffer[i + start];
                    switch (ch) {
                        case '0':
                        case '1':
                        case '2':
                        case '3':
                        case '4':
                        case '5':
                        case '6':
                        case '7':
                        case '8':
                        case '9':
                            n = n * 10 + (ch - '0');
                            break;
                        default:
                            String errorString = new String(buffer, i + start, length - i);
                            throw new HyracksDataException(
                                    "Integer Parser - a digit is expected. But, encountered this character: " + ch
                                            + " in the incoming input: " + errorString);
                    }
                }

                for (; i < length; ++i) {
                    char ch = buffer[i + start];
                    switch (ch) {
                        case ' ':
                        case '\t':
                        case '\n':
                        case '\r':
                        case '\f':
                            break;

                        default:
                            String errorString = new String(buffer, i + start, length - i);
                            throw new HyracksDataException("Integer Parser - a whitespace, tab, new line, or "
                                    + "form-feed expected. But, encountered this character: " + ch
                                    + " in the incoming input: " + errorString);
                    }
                }

                try {
                    out.writeInt(n * sign);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
        };
    }
}
