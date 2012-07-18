/**
 * 
 */
package edu.uci.ics.asterix.om.types;

/**
 * There is a unique tag for each primitive type and for each kind of
 * non-primitive type in the object model.
 * 
 * @author Nicola
 */
public enum ATypeTag implements IEnumSerializer {
    INT8(1),
    INT16(2),
    INT32(3),
    INT64(4),
    UINT8(5),
    UINT16(6),
    UINT32(7),
    UINT64(8),
    BINARY(9),
    BITARRAY(10),
    FLOAT(11),
    DOUBLE(12),
    STRING(13),
    NULL(14),    
    BOOLEAN(15),
    DATETIME(16),
    DATE(17),
    TIME(18),
    DURATION(19),
    POINT(20),
    POINT3D(21),
    ORDEREDLIST(22),
    UNORDEREDLIST(23),
    RECORD(24),
    SPARSERECORD(25),
    UNION(26),
    ENUM(27),
    TYPE(28),
    ANY(29),
    LINE(30),
    POLYGON(31),
    CIRCLE(32),
    RECTANGLE(33),
    SYSTEM_NULL(34);

    private byte value;

    private ATypeTag(int value) {
        this.value = (byte) value;
    }

    @Override
    public byte serialize() {
        return value;
    }

}