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
package edu.uci.ics.hyracks.algebricks.data.impl;

import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.utils.WriteValueTools;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public class IntegerPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final IntegerPrinterFactory INSTANCE = new IntegerPrinterFactory();

    private IntegerPrinterFactory() {
    }

    @Override
    public IPrinter createPrinter() {
        return new IPrinter() {

            @Override
            public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                int d = IntegerPointable.getInteger(b, s);
                try {
                    WriteValueTools.writeInt(d, ps);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }

            @Override
            public void init() throws AlgebricksException {
            }
        };
    }

}
