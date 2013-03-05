package edu.uci.ics.asterix.om.types;

import edu.uci.ics.asterix.om.base.IAObject;

public class AUnorderedListType extends AbstractCollectionType {

    private static final long serialVersionUID = 1L;

    /**
     * @param itemType
     *            if null, the collection is untyped
     */
    public AUnorderedListType(IAType itemType, String typeName) {
        super(itemType, typeName);
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.UNORDEREDLIST;
    }

    @Override
    public String getDisplayName() {
        return "AUnorderedList";
    }

    @Override
    public String toString() {
        return "{ " + itemType + " }";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AUnorderedListType) {
            AUnorderedListType type = (AUnorderedListType) obj;
            return this.itemType == type.itemType;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.itemType.hashCode() * 10;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

}
