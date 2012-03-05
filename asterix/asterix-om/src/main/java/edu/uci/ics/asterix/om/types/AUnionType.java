package edu.uci.ics.asterix.om.types;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AUnionType extends AbstractComplexType {

    private static final long serialVersionUID = 1L;
    private List<IAType> unionList;

    public AUnionType(List<IAType> unionList, String typeName) {
        super(typeName);
        this.unionList = unionList;
    }

    public List<IAType> getUnionList() {
        return unionList;
    }

    public void setTypeAtIndex(IAType type, int index) {
        unionList.set(index, type);
    }

    public boolean isNullableType() {
        return unionList.size() == 2 && unionList.get(0).equals(BuiltinType.ANULL);
    }

    @Override
    public String getDisplayName() {
        return "AUnion";
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.UNION;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("UNION(");
        Iterator<IAType> iter = unionList.iterator();
        if (iter.hasNext()) {
            IAType t0 = iter.next();
            sb.append(t0.toString());
            while (iter.hasNext()) {
                sb.append(", " + iter.next());
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAType(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.ASTERIX_TYPE;
    }

    public static AUnionType createNullableType(IAType t) {
        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);
        unionList.add(t);
        String s = t.getDisplayName();
        return new AUnionType(unionList, s == null ? null : s + "?");
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AUnionType)) {
            return false;
        }
        AUnionType ut = (AUnionType) obj;
        if (ut.getUnionList().size() != unionList.size()) {
            return false;
        }
        for (int i = 0; i < unionList.size(); i++) {
            if (!unionList.get(i).deepEqual(ut.getUnionList().get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hash() {
        int h = 0;
        for (IAType t : unionList) {
            h += 31 * h + t.hash();
        }
        return h;
    }

}
