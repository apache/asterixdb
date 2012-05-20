package edu.uci.ics.asterix.runtime.accessors.cast;

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
import edu.uci.ics.asterix.runtime.accessors.AFlatValueAccessor;
import edu.uci.ics.asterix.runtime.accessors.AListAccessor;
import edu.uci.ics.asterix.runtime.accessors.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.runtime.accessors.base.IBinaryAccessor;
import edu.uci.ics.asterix.runtime.util.ResettableByteArrayOutputStream;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;

class AListCaster {

    private IAType reqItemType;
    private IBinaryAccessor itemTempReference = AFlatValueAccessor.FACTORY.createElement(null);
    private Triple<IBinaryAccessor, IAType, Boolean> itemVisitorArg = new Triple<IBinaryAccessor, IAType, Boolean>(
            itemTempReference, null, null);

    private UnorderedListBuilder unOrderedListBuilder = new UnorderedListBuilder();
    private OrderedListBuilder orderedListBuilder = new OrderedListBuilder();

    private byte[] dataBuffer = new byte[32768];
    private ResettableByteArrayOutputStream dataBos = new ResettableByteArrayOutputStream();
    private DataOutput dataDos = new DataOutputStream(dataBos);

    public AListCaster() {

    }

    public void castList(AListAccessor listAccessor, IBinaryAccessor resultAccessor, AbstractCollectionType reqType,
            ACastVisitor visitor) throws IOException, AsterixException {
        if (reqType.getTypeTag().equals(ATypeTag.UNORDEREDLIST)) {
            unOrderedListBuilder.reset((AUnorderedListType) reqType);
        }
        if (reqType.getTypeTag().equals(ATypeTag.ORDEREDLIST)) {
            orderedListBuilder.reset((AOrderedListType) reqType);
        }
        dataBos.setByteArray(dataBuffer, 0);

        List<IBinaryAccessor> itemTags = listAccessor.getItemTags();
        List<IBinaryAccessor> items = listAccessor.getItems();

        int start = dataBos.size();
        for (int i = 0; i < items.size(); i++) {
            IBinaryAccessor itemTypeTag = itemTags.get(i);
            IBinaryAccessor item = items.get(i);
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(itemTypeTag.getBytes()[itemTypeTag
                    .getStartIndex()]);
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
        resultAccessor.reset(dataBuffer, start, end - start);
    }
}
