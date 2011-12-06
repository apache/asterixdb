/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.data;

import java.io.PrintStream;

import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;

public class UTF8StringPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;

    public static final UTF8StringPrinterFactory INSTANCE = new UTF8StringPrinterFactory();

    private UTF8StringPrinterFactory() {
    }

    @Override
    public IPrinter createPrinter() {
        return new IPrinter() {

            @Override
            public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                int strlen = StringUtils.getUTFLen(b, s);
                int pos = s + 2;
                int maxPos = pos + strlen;
                ps.print("\"");
                while (pos < maxPos) {
                    char c = StringUtils.charAt(b, pos);
                    switch (c) {
                        case '\\':
                        case '"':
                            ps.print('\\');
                            break;
                    }
                    ps.print(c);
                    pos += StringUtils.charSize(b, pos);
                }
                ps.print("\"");
            }

            @Override
            public void init() throws AlgebricksException {
            }
        };
    }

}
