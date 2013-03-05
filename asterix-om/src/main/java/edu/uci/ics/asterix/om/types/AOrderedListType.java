package edu.uci.ics.asterix.om.types;

import edu.uci.ics.asterix.om.base.IAObject;

public class AOrderedListType extends AbstractCollectionType {

    private static final long serialVersionUID = 1L;

    /**
     * @param itemType
     *            if null, the list is untyped
     */
    public AOrderedListType(IAType itemType, String typeName) {
        super(itemType, typeName);
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.ORDEREDLIST;
    }

    @Override
    public String getDisplayName() {
        return "AOrderedList";
    }

    @Override
    public String toString() {
        return "[ " + itemType + " ]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AOrderedListType) {
            AOrderedListType type = (AOrderedListType) obj;
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
