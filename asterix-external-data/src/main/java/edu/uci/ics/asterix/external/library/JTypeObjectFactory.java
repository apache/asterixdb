package edu.uci.ics.asterix.external.library;

import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JInt;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectFactory;

public class JTypeObjectFactory implements IObjectFactory<IJObject, IAType> {

    @Override
    public IJObject create(IAType type) {
        IJObject retValue = null;
        switch (type.getTypeTag()) {
            case INT32:
                retValue = new JInt(0);
                break;
            case STRING:
                retValue = new JString("");
                break;
            case RECORD:
                IAType[] fieldTypes = ((ARecordType) type).getFieldTypes();
                IJObject[] fieldObjects = new IJObject[fieldTypes.length];
                int index = 0;
                for (IAType fieldType : fieldTypes) {
                    fieldObjects[index] = create(fieldType);
                    index++;
                }
                retValue = new JRecord((ARecordType) type, fieldObjects);

                break;
        }
        return retValue;
    }
}
