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

import java.io.PrintStream;

import org.apache.asterix.formats.nontagged.LosslessADMJSONPrinterFactoryProvider;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AOptionalFieldPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    private final AUnionType unionType;

    public AOptionalFieldPrinterFactory(AUnionType unionType) {
        this.unionType = unionType;
    }

    @Override
    public IPrinter createPrinter(IEvaluatorContext context) {
        return new IPrinter() {
            private IPrinter missingPrinter;
            private IPrinter nullPrinter;
            private IPrinter fieldPrinter;

            @Override
            public void init() throws HyracksDataException {
                missingPrinter =
                        (LosslessADMJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(BuiltinType.AMISSING))
                                .createPrinter(context);
                nullPrinter = (LosslessADMJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(BuiltinType.ANULL))
                        .createPrinter(context);
                fieldPrinter =
                        (LosslessADMJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(unionType.getActualType()))
                                .createPrinter(context);
            }

            @Override
            public void print(byte[] b, int s, int l, PrintStream ps) throws HyracksDataException {
                fieldPrinter.init();
                if (b[s] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                    missingPrinter.print(b, s, l, ps);
                } else if (b[s] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                    nullPrinter.print(b, s, l, ps);
                } else {
                    fieldPrinter.print(b, s, l, ps);
                }
            }
        };
    }
}
