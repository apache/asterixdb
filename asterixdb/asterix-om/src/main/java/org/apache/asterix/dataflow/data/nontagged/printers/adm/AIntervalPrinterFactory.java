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
package org.apache.asterix.dataflow.data.nontagged.printers.adm;

import java.io.PrintStream;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AIntervalPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final AIntervalPrinterFactory INSTANCE = new AIntervalPrinterFactory();

    public static final IPrinter PRINTER = (byte[] b, int s, int l, PrintStream ps) -> {
        ps.print("interval(");
        byte typetag = AIntervalSerializerDeserializer.getIntervalTimeType(b, s + 1);
        int startOffset = AIntervalSerializerDeserializer.getIntervalStartOffset(s + 1) - 1;
        int startSize = AIntervalSerializerDeserializer.getStartSize(b, s + 1);
        int endOffset = AIntervalSerializerDeserializer.getIntervalEndOffset(b, s + 1) - 1;
        int endSize = AIntervalSerializerDeserializer.getEndSize(b, s + 1);
        IPrinter timeInstancePrinter = getIPrinter(typetag);
        timeInstancePrinter.print(b, startOffset, startSize, ps);
        ps.print(", ");
        timeInstancePrinter.print(b, endOffset, endSize, ps);
        ps.print(")");
    };

    private static IPrinter getIPrinter(byte typetag) throws HyracksDataException {
        switch (EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(typetag)) {
            case DATE:
                return ADatePrinterFactory.PRINTER;
            case TIME:
                return ATimePrinterFactory.PRINTER;
            case DATETIME:
                return ADateTimePrinterFactory.PRINTER;
            default:
                throw new HyracksDataException("Unsupported internal time types in interval: " + typetag);
        }
    }

    @Override
    public IPrinter createPrinter() {
        return PRINTER;
    }
}
