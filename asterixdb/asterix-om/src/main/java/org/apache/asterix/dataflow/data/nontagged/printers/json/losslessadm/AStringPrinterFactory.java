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

package org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm;

import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.printers.PrintTools;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.string.UTF8StringUtil;

/*
 * String value printed without type code.
 * Empty string is printed as empty string.
 */
public class AStringPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;

    public static final AStringPrinterFactory INSTANCE = new AStringPrinterFactory();

    public static final IPrinter PRINTER = (b, s, l, ps) -> {
        try {
            int utfLength = UTF8StringUtil.getUTFLength(b, s + 1);
            ps.print('"');
            if (utfLength > 0) {
                ATaggedValuePrinter.printDelimiter(ps);
                PrintTools.writeUTF8StringAsJSONUnquoted(b, s + 1, l - 1, utfLength, ps);
            }
            ps.print('"');

        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    };

    @Override
    public IPrinter createPrinter() {
        return PRINTER;
    }
}
