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
package org.apache.asterix.dataflow.data.nontagged.printers.csv;

import java.io.PrintStream;
import java.util.Map;

import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.printer.csv.APrintVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ARecordPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    private final ARecordType recType;
    private final ARecordType itemType;
    private final Map<String, String> formatConfigs;
    private final Map<String, String> configuration;

    public ARecordPrinterFactory(ARecordType recType, ARecordType itemType, Map<String, String> formatConfigs,
            Map<String, String> configuration) {
        this.recType = recType;
        this.itemType = itemType;
        this.formatConfigs = formatConfigs;
        this.configuration = configuration;
    }

    @Override
    public IPrinter createPrinter(IEvaluatorContext context) {

        final PointableAllocator allocator = new PointableAllocator();
        final IAType inputType =
                recType == null ? DefaultOpenFieldType.getDefaultOpenFieldType(ATypeTag.OBJECT) : recType;
        final IVisitablePointable recAccessor = allocator.allocateRecordValue(inputType);
        final APrintVisitor printVisitor = new APrintVisitor(context, itemType, formatConfigs, configuration);
        final Pair<PrintStream, ATypeTag> arg = new Pair<>(null, null);

        return new IPrinter() {
            @Override
            public void init() {
                arg.second = inputType.getTypeTag();
            }

            @Override
            public void print(byte[] b, int start, int l, PrintStream ps) throws HyracksDataException {
                recAccessor.set(b, start, l);
                arg.first = ps;
                recAccessor.accept(printVisitor, arg);
            }
        };
    }
}
