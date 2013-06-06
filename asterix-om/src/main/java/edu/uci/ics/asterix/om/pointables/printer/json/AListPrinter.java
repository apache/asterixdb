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
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

/**
 * This class is to print the content of a list. It is ONLY visible to
 * APrintVisitor.
 */
class AListPrinter {
    private static String BEGIN = "{ unorderedlist: [";
    private static String BEGIN_ORDERED = "{ orderedlist: [";
    private static String END = " ]}";
    private static String COMMA = ", ";

    private final Pair<PrintStream, ATypeTag> itemVisitorArg = new Pair<PrintStream, ATypeTag>(null, null);
    private String begin = BEGIN;

    public AListPrinter(boolean ordered) {
        if (ordered) {
            begin = BEGIN_ORDERED;
        }
    }

    public void printList(AListPointable listAccessor, PrintStream ps, APrintVisitor visitor) throws IOException,
            AsterixException {
        List<IVisitablePointable> itemTags = listAccessor.getItemTags();
        List<IVisitablePointable> items = listAccessor.getItems();
        itemVisitorArg.first = ps;

        // print the beginning part
        ps.print(begin);

        // print item 0 to n-2
        for (int i = 0; i < items.size() - 1; i++) {
            printItem(visitor, itemTags, items, i);
            // print the comma
            ps.print(COMMA);
        }

        // print item n-1
        if (items.size() > 0) {
            printItem(visitor, itemTags, items, items.size() - 1);
        }

        // print the end part
        ps.print(END);
    }

    private void printItem(APrintVisitor visitor, List<IVisitablePointable> itemTags, List<IVisitablePointable> items,
            int i) throws AsterixException {
        IVisitablePointable itemTypeTag = itemTags.get(i);
        IVisitablePointable item = items.get(i);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(itemTypeTag.getByteArray()[itemTypeTag
                .getStartOffset()]);
        itemVisitorArg.second = item.getLength() <= 1 ? ATypeTag.NULL : typeTag;
        item.accept(visitor, itemVisitorArg);
    }
}
