package edu.uci.ics.asterix.om.types;

import java.util.HashMap;
import java.util.Map;

public class EnumDeserializer<E extends Enum<E> & IEnumSerializer> {

    public static final EnumDeserializer<ATypeTag> ATYPETAGDESERIALIZER = new EnumDeserializer<ATypeTag>(ATypeTag.class);

    private Map<Byte, E> enumvalMap = new HashMap<Byte, E>();

    private EnumDeserializer(Class<E> enumClass) {
        for (E constant : enumClass.getEnumConstants()) {
            enumvalMap.put(constant.serialize(), constant);
        }
    }

    public E deserialize(byte value) {
        return enumvalMap.get(value);
    }

}
