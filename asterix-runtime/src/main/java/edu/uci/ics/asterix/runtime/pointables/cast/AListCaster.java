/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.runtime.pointables.cast;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.UnorderedListBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.runtime.pointables.AListPointable;
import edu.uci.ics.asterix.runtime.pointables.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.runtime.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.runtime.util.ResettableByteArrayOutputStream;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;

class AListCaster {

    private IAType reqItemType;
    private IVisitablePointable itemTempReference = AFlatValuePointable.FACTORY.createElement(null);
    private Triple<IVisitablePointable, IAType, Boolean> itemVisitorArg = new Triple<IVisitablePointable, IAType, Boolean>(
            itemTempReference, null, null);

    private UnorderedListBuilder unOrderedListBuilder = new UnorderedListBuilder();
    private OrderedListBuilder orderedListBuilder = new OrderedListBuilder();

    private byte[] dataBuffer = new byte[32768];
    private ResettableByteArrayOutputStream dataBos = new ResettableByteArrayOutputStream();
    private DataOutput dataDos = new DataOutputStream(dataBos);

    public AListCaster() {

    }

    public void castList(AListPointable listAccessor, IVisitablePointable resultAccessor, AbstractCollectionType reqType,
            ACastVisitor visitor) throws IOException, AsterixException {
        if (reqType.getTypeTag().equals(ATypeTag.UNORDEREDLIST)) {
            unOrderedListBuilder.reset((AUnorderedListType) reqType);
        }
        if (reqType.getTypeTag().equals(ATypeTag.ORDEREDLIST)) {
            orderedListBuilder.reset((AOrderedListType) reqType);
        }
        dataBos.setByteArray(dataBuffer, 0);

        List<IVisitablePointable> itemTags = listAccessor.getItemTags();
        List<IVisitablePointable> items = listAccessor.getItems();

        int start = dataBos.size();
        for (int i = 0; i < items.size(); i++) {
            IVisitablePointable itemTypeTag = itemTags.get(i);
            IVisitablePointable item = items.get(i);
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(itemTypeTag.getByteArray()[itemTypeTag
                    .getStartOffset()]);
            if (reqItemType == null || reqItemType.getTypeTag().equals(ATypeTag.ANY)) {
                itemVisitorArg.second = DefaultOpenFieldType.getDefaultOpenFieldType(typeTag);
                item.accept(visitor, itemVisitorArg);
            } else {
                if (typeTag != reqItemType.getTypeTag())
                    throw new AsterixException("mismatched item type");
                itemVisitorArg.second = reqItemType;
                item.accept(visitor, itemVisitorArg);
            }
            if (reqType.getTypeTag().equals(ATypeTag.ORDEREDLIST)) {
                orderedListBuilder.addItem(itemVisitorArg.first);
            }
            if (reqType.getTypeTag().equals(ATypeTag.UNORDEREDLIST)) {
                unOrderedListBuilder.addItem(itemVisitorArg.first);
            }
        }
        if (reqType.getTypeTag().equals(ATypeTag.ORDEREDLIST)) {
            orderedListBuilder.write(dataDos, true);
        }
        if (reqType.getTypeTag().equals(ATypeTag.UNORDEREDLIST)) {
            unOrderedListBuilder.write(dataDos, true);
        }
        int end = dataBos.size();
        resultAccessor.set(dataBuffer, start, end - start);
    }
}
