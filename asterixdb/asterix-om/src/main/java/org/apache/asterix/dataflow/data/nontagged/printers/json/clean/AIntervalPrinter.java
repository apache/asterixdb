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

import java.io.PrintStream;

import org.apache.asterix.dataflow.data.nontagged.printers.adm.ADatePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.adm.ADateTimePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.adm.ATimePrinter;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AIntervalPrinter implements IPrinter {

    public static final AIntervalPrinter INSTANCE = new AIntervalPrinter();

    /* (non-Javadoc)
     * @see org.apache.hyracks.algebricks.data.IPrinter#init()
     */
    @Override
    public void init() {
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.algebricks.data.IPrinter#print(byte[], int, int, java.io.PrintStream)
     */
    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws HyracksDataException {
        ps.print("{ \"interval\": { \"start\": ");

        byte typetag = AIntervalSerializerDeserializer.getIntervalTimeType(b, s + 1);
        int startOffset = AIntervalSerializerDeserializer.getIntervalStartOffset(b, s + 1) - 1;
        int startSize = AIntervalSerializerDeserializer.getStartSize(b, s + 1);
        int endOffset = AIntervalSerializerDeserializer.getIntervalEndOffset(b, s + 1) - 1;
        int endSize = AIntervalSerializerDeserializer.getEndSize(b, s + 1);

        IPrinter timeInstancePrinter;
        ATypeTag intervalType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(typetag);
        switch (intervalType) {
            case DATE:
                timeInstancePrinter = ADatePrinter.INSTANCE;
                break;
            case TIME:
                timeInstancePrinter = ATimePrinter.INSTANCE;
                break;
            case DATETIME:
                timeInstancePrinter = ADateTimePrinter.INSTANCE;
                break;
            default:
                throw new HyracksDataException("Unsupported internal time types in interval: " + typetag);
        }

        timeInstancePrinter.print(b, startOffset, startSize, ps);
        ps.print(", \"end\": ");
        timeInstancePrinter.print(b, endOffset, endSize, ps);

        ps.print("}}");
    }
}
