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

package org.apache.asterix.om.pointables.printer;

import java.io.PrintStream;
import java.util.List;

import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This class is to print the content of a record.
 */
public class ARecordPrinter {
    private final String startRecord;
    private final String endRecord;
    private final String fieldSeparator;
    private final String fieldNameSeparator;

    private final Pair<PrintStream, ATypeTag> nameVisitorArg = new Pair<>(null, ATypeTag.STRING);
    private final Pair<PrintStream, ATypeTag> itemVisitorArg = new Pair<>(null, null);

    public ARecordPrinter(final String startRecord, final String endRecord, final String fieldSeparator,
            final String fieldNameSeparator) {
        this.startRecord = startRecord;
        this.endRecord = endRecord;
        this.fieldSeparator = fieldSeparator;
        this.fieldNameSeparator = fieldNameSeparator;
    }

    public void printRecord(ARecordVisitablePointable recordAccessor, PrintStream ps, IPrintVisitor visitor)
            throws HyracksDataException {
        final List<IVisitablePointable> fieldNames = recordAccessor.getFieldNames();
        final List<IVisitablePointable> fieldValues = recordAccessor.getFieldValues();

        nameVisitorArg.first = ps;
        itemVisitorArg.first = ps;

        ps.print(startRecord);

        final int size = fieldNames.size();
        boolean first = true;
        for (int i = 0; i < size; ++i) {
            final IVisitablePointable fieldName = fieldNames.get(i);
            final IVisitablePointable fieldValue = fieldValues.get(i);
            final ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(fieldValue.getByteArray()[fieldValue.getStartOffset()]);

            // Prints the current field.
            if (typeTag != ATypeTag.MISSING) {
                if (first) {
                    //Skip printing field separator for the first field.
                    first = false;
                } else {
                    ps.print(fieldSeparator);
                }
                printField(ps, visitor, fieldName, fieldValue, typeTag);
            }
        }

        ps.print(endRecord);
    }

    private void printField(PrintStream ps, IPrintVisitor visitor, IVisitablePointable fieldName,
            IVisitablePointable fieldValue, ATypeTag fieldTypeTag) throws HyracksDataException {
        itemVisitorArg.second = fieldTypeTag;
        if (fieldNameSeparator != null) {
            // print field name
            fieldName.accept(visitor, nameVisitorArg);
            ps.print(fieldNameSeparator);
        }
        // print field value
        fieldValue.accept(visitor, itemVisitorArg);
    }
}
