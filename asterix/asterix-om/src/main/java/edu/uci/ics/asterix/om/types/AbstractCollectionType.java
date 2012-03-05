package edu.uci.ics.asterix.om.types;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public abstract class AbstractCollectionType extends AbstractComplexType {

    private static final long serialVersionUID = 1L;

    protected IAType itemType;

    AbstractCollectionType(IAType itemType, String typeName) {
        super(typeName);
        this.itemType = itemType;
    }

    public boolean isTyped() {
        return itemType != null;
    }

    public IAType getItemType() {
        return itemType;
    }

    public void setItemType(IAType itemType) {
        this.itemType = itemType;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ASTERIX_TYPE;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAType(this);
    }

    // public void serialize(DataOutput out) throws IOException {
    // out.writeBoolean(isTyped());
    // }

}
