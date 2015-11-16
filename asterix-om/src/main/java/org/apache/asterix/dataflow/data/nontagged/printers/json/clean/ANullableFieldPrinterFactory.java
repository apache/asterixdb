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
package org.apache.asterix.dataflow.data.nontagged.printers.json.clean;

import org.apache.asterix.formats.nontagged.AqlCleanJSONPrinterFactoryProvider;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;

import java.io.PrintStream;

public class ANullableFieldPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;

    private AUnionType unionType;

    public ANullableFieldPrinterFactory(AUnionType unionType) {
        this.unionType = unionType;
    }

    @Override
    public IPrinter createPrinter() {
        return new IPrinter() {
            private IPrinter nullPrinter;
            private IPrinter fieldPrinter;

            @Override
            public void init() throws AlgebricksException {
                nullPrinter = (AqlCleanJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(BuiltinType.ANULL))
                        .createPrinter();
                fieldPrinter = (AqlCleanJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(unionType.getNullableType()))
                        .createPrinter();
            }

            @Override
            public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                fieldPrinter.init();
                if (b[s] == ATypeTag.NULL.serialize())
                    nullPrinter.print(b, s, l, ps);
                else
                    fieldPrinter.print(b, s, l, ps);
            }

        };
    }

}
