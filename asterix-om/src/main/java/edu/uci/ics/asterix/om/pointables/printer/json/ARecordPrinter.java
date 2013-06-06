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

package edu.uci.ics.asterix.om.pointables.printer.json;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

/**
 * This class is to print the content of a record. It is ONLY visible to
 * APrintVisitor.
 */
class ARecordPrinter {
    private static String LEFT_PAREN = "{ ";
    private static String RIGHT_PAREN = " }";
    private static String COMMA = ", ";
    private static String COLON = ": ";

    private final Pair<PrintStream, ATypeTag> nameVisitorArg = new Pair<PrintStream, ATypeTag>(null, ATypeTag.STRING);
    private final Pair<PrintStream, ATypeTag> itemVisitorArg = new Pair<PrintStream, ATypeTag>(null, null);

    public ARecordPrinter() {

    }

    public void printRecord(ARecordPointable recordAccessor, PrintStream ps, APrintVisitor visitor) throws IOException,
            AsterixException {
        List<IVisitablePointable> fieldNames = recordAccessor.getFieldNames();
        List<IVisitablePointable> fieldTags = recordAccessor.getFieldTypeTags();
        List<IVisitablePointable> fieldValues = recordAccessor.getFieldValues();

        nameVisitorArg.first = ps;
        itemVisitorArg.first = ps;

        // print the beginning part
        ps.print(LEFT_PAREN);

        // print field 0 to n-2
        for (int i = 0; i < fieldNames.size() - 1; i++) {
            printField(ps, visitor, fieldNames, fieldTags, fieldValues, i);
            // print the comma
            ps.print(COMMA);
        }

        // print field n-1
        if (fieldValues.size() > 0) {
            printField(ps, visitor, fieldNames, fieldTags, fieldValues, fieldValues.size() - 1);
        }

        // print the end part
        ps.print(RIGHT_PAREN);
    }

    private void printField(PrintStream ps, APrintVisitor visitor, List<IVisitablePointable> fieldNames,
            List<IVisitablePointable> fieldTags, List<IVisitablePointable> fieldValues, int i) throws AsterixException {
        IVisitablePointable itemTypeTag = fieldTags.get(i);
        IVisitablePointable item = fieldValues.get(i);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(itemTypeTag.getByteArray()[itemTypeTag
                .getStartOffset()]);
        itemVisitorArg.second = item.getLength() <= 1 ? ATypeTag.NULL : typeTag;

        // print field name
        fieldNames.get(i).accept(visitor, nameVisitorArg);
        ps.print(COLON);
        // print field value
        item.accept(visitor, itemVisitorArg);
    }
}
