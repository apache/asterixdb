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

    public static void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
        try {
            if (length == 4 && (buffer[start] == 't' || buffer[start] == 'T')
                    && (buffer[start + 1] == 'r' || buffer[start + 1] == 'R')
                    && (buffer[start + 2] == 'u' || buffer[start + 2] == 'U')
                    && (buffer[start + 3] == 'e' || buffer[start + 3] == 'E')) {
                out.writeBoolean(true);
                return;
            } else if (length == 5 && (buffer[start] == 'f' || buffer[start] == 'F')
                    && (buffer[start + 1] == 'a' || buffer[start + 1] == 'A')
                    && (buffer[start + 2] == 'l' || buffer[start + 2] == 'L')
                    && (buffer[start + 3] == 's' || buffer[start + 3] == 'S')
                    && (buffer[start + 4] == 'e' || buffer[start + 4] == 'E')) {
                out.writeBoolean(false);
                return;
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

        throw new HyracksDataException("Invalid input data");
    }
}
