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

import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This class is to print the content of a list.
 */
public class AListPrinter {
    private final String startList;
    private final String endList;
    private final String separator;

    private final Pair<PrintStream, ATypeTag> itemVisitorArg = new Pair<>(null, null);

    public AListPrinter(String startList, String endList, String separator) {
        this.startList = startList;
        this.endList = endList;
        this.separator = separator;
    }

    public void printList(AListVisitablePointable listAccessor, PrintStream ps, IPrintVisitor visitor)
            throws HyracksDataException {
        List<IVisitablePointable> itemTags = listAccessor.getItemTags();
        List<IVisitablePointable> items = listAccessor.getItems();
        itemVisitorArg.first = ps;

        ps.print(startList);

        // print item 0 to n-2
        final int size = items.size();
        for (int i = 0; i < size - 1; i++) {
            printItem(visitor, itemTags, items, i);
            ps.print(separator);
        }

        // print item n-1
        if (size > 0) {
            printItem(visitor, itemTags, items, size - 1);
        }

        ps.print(endList);
    }

    private void printItem(IPrintVisitor visitor, List<IVisitablePointable> itemTags, List<IVisitablePointable> items,
            int i) throws HyracksDataException {
        IVisitablePointable itemTypeTag = itemTags.get(i);
        IVisitablePointable item = items.get(i);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                .deserialize(itemTypeTag.getByteArray()[itemTypeTag.getStartOffset()]);
        itemVisitorArg.second = item.getLength() <= 1 ? ATypeTag.NULL : typeTag;
        item.accept(visitor, itemVisitorArg);
    }
}
