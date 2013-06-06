/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.common.data.parsers;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class DoubleParserFactory implements IValueParserFactory {
    public static final IValueParserFactory INSTANCE = new DoubleParserFactory();

    private static final long serialVersionUID = 1L;

    private DoubleParserFactory() {
    }

    @Override
    public IValueParser createValueParser() {
        return new IValueParser() {
            @Override
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                String s = String.valueOf(buffer, start, length);
                try {
                    out.writeDouble(Double.parseDouble(s));
                } catch (NumberFormatException e) {
                    throw new HyracksDataException(e);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}