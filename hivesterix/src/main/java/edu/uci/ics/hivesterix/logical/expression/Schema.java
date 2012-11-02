package edu.uci.ics.hivesterix.logical.expression;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import edu.uci.ics.hivesterix.serde.lazy.LazyUtils;

public class Schema implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<String> fieldNames;

    private List<TypeInfo> fieldTypes;

    public Schema(List<String> fieldNames, List<TypeInfo> fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    public ObjectInspector toObjectInspector() {
        return LazyUtils.getLazyObjectInspector(fieldNames, fieldTypes);
    }

    public List<String> getNames() {
        return fieldNames;
    }

    public List<TypeInfo> getTypes() {
        return fieldTypes;
    }

    public Object[] getSchema() {
        return fieldTypes.toArray();
    }
}
