package edu.uci.ics.asterix.om.types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.common.annotations.IRecordTypeAnnotation;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ARecordType extends AbstractComplexType {

    private static final long serialVersionUID = 1L;
    private String[] fieldNames;
    private IAType[] fieldTypes;
    private boolean isOpen;
    private final List<IRecordTypeAnnotation> annotations = new ArrayList<IRecordTypeAnnotation>();
    private final Map<String, Integer> typeMap = new HashMap<String, Integer>();

    public ARecordType(String typeName, String[] fieldNames, IAType[] fieldTypes, boolean isOpen) {
        super(typeName);
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.isOpen = isOpen;
        for (int i = 0; i < fieldNames.length; i++) {
            typeMap.put(fieldNames[i], i);
        }
    }

    public final String[] getFieldNames() {
        return fieldNames;
    }

    public final IAType[] getFieldTypes() {
        return fieldTypes;
    }

    public List<IRecordTypeAnnotation> getAnnotations() {
        return annotations;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (isOpen) {
            sb.append("open ");
        } else {
            sb.append("closed ");
        }
        sb.append("{\n");
        int n = fieldNames.length;
        for (int i = 0; i < n; i++) {
            sb.append("  " + fieldNames[i] + ": " + fieldTypes[i].toString());
            if (i < n - 1) {
                sb.append(",\n");
            } else {
                sb.append("\n");
            }
        }
        sb.append("}\n");
        return sb.toString();
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.RECORD;
    }

    public boolean isOpen() {
        return isOpen;
    }

    public int findFieldPosition(String fldName) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(fldName)) {
                return i;
            }
        }
        return -1;
    }

    public IAType getFieldType(String fieldName) {
        return fieldTypes[typeMap.get(fieldName)];
    }

    @Override
    public String getDisplayName() {
        return "ARecord";
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAType(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.ASTERIX_TYPE;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ARecordType)) {
            return false;
        }
        ARecordType rt = (ARecordType) obj;
        return isOpen == rt.isOpen && Arrays.deepEquals(fieldNames, rt.fieldNames)
                && Arrays.deepEquals(fieldTypes, rt.fieldTypes);
    }

    @Override
    public int hash() {
        int h = 0;
        for (int i = 0; i < fieldNames.length; i++) {
            h += 31 * h + fieldNames[i].hashCode();
        }
        for (int i = 0; i < fieldTypes.length; i++) {
            h += 31 * h + fieldTypes[i].hashCode();
        }
        return h;
    }

}
