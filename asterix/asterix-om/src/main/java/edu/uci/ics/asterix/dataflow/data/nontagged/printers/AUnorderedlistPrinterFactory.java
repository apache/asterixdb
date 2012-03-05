package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlPrinterFactoryProvider;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class AUnorderedlistPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;

    private AUnorderedListType unorderedlistType;

    public AUnorderedlistPrinterFactory(AUnorderedListType unorderedlistType) {
        this.unorderedlistType = unorderedlistType;
    }

    @Override
    public IPrinter createPrinter() {

        return new IPrinter() {

            private IPrinter itemPrinter;
            private IAType itemType;
            private ATypeTag itemTag;
            private boolean typedItemList = false;

            @Override
            public void init() throws AlgebricksException {

                if (unorderedlistType != null && unorderedlistType.getItemType() != null) {
                    itemType = unorderedlistType.getItemType();
                    if (itemType.getTypeTag() == ATypeTag.ANY) {
                        this.typedItemList = false;
                        this.itemPrinter = AObjectPrinterFactory.INSTANCE.createPrinter();
                    } else {
                        this.typedItemList = true;
                        itemPrinter = AqlPrinterFactoryProvider.INSTANCE.getPrinterFactory(itemType).createPrinter();
                        itemTag = unorderedlistType.getItemType().getTypeTag();
                    }
                } else {
                    this.typedItemList = false;
                    this.itemPrinter = AObjectPrinterFactory.INSTANCE.createPrinter();
                }
                itemPrinter.init();
            }

            @Override
            public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                ps.print("{{ ");
                int numberOfitems = AInt32SerializerDeserializer.getInt(b, s + 6);
                int itemOffset;
                if (typedItemList) {
                    switch (itemTag) {
                        case STRING:
                        case RECORD:
                        case ORDEREDLIST:
                        case UNORDEREDLIST:
                        case ANY:
                            itemOffset = s + 10 + (numberOfitems * 4);
                            break;
                        default:
                            itemOffset = s + 10;
                    }
                } else
                    itemOffset = s + 10 + (numberOfitems * 4);
                int itemLength;

                try {
                    if (typedItemList) {
                        for (int i = 0; i < numberOfitems; i++) {
                            itemLength = NonTaggedFormatUtil.getFieldValueLength(b, itemOffset, itemTag, false);
                            itemPrinter.print(b, itemOffset - 1, itemLength, ps);
                            itemOffset += itemLength;
                            if (i + 1 < numberOfitems)
                                ps.print(", ");
                        }
                    } else {
                        for (int i = 0; i < numberOfitems; i++) {
                            itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[itemOffset]);
                            itemLength = NonTaggedFormatUtil.getFieldValueLength(b, itemOffset, itemTag, true) + 1;
                            itemPrinter.print(b, itemOffset, itemLength, ps);
                            itemOffset += itemLength;
                            if (i + 1 < numberOfitems)
                                ps.print(", ");
                        }
                    }
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }
                ps.print(" }}");
            }
        };
    }
}
