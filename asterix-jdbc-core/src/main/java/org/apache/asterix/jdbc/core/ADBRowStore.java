/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.jdbc.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;

final class ADBRowStore {

    private static final String ROW_STORE_ATTR_NAME = ADBRowStore.class.getSimpleName();

    private static final int FLOAT_NAN_BITS = Float.floatToIntBits(Float.NaN);
    private static final int FLOAT_POSITIVE_ZERO_BITS = Float.floatToIntBits(+0.0f);
    private static final int FLOAT_NEGATIVE_ZERO_BITS = Float.floatToIntBits(-0.0f);
    private static final long DOUBLE_NAN_BITS = Double.doubleToLongBits(Double.NaN);
    private static final long DOUBLE_POSITIVE_ZERO_BITS = Double.doubleToLongBits(+0.0d);
    private static final long DOUBLE_NEGATIVE_ZERO_BITS = Double.doubleToLongBits(-0.0d);

    static final Map<Class<?>, GetObjectFunction> OBJECT_ACCESSORS_ATOMIC = createAtomicObjectAccessorMap();

    static final List<Class<?>> GET_OBJECT_NON_ATOMIC = Arrays.asList(Collection.class, List.class, Map.class);

    private final ADBResultSet resultSet;

    private final ADBDatatype[] columnTypes;
    private final Object[] objectStore;
    private final long[] registerStore; // 2 registers per column

    private int parsedLength;
    private long currentDateChronon;
    private JsonGenerator jsonGen;
    private StringWriter jsonGenBuffer;

    ADBRowStore(ADBResultSet resultSet, int initialColumnCount) {
        this.resultSet = Objects.requireNonNull(resultSet);
        columnTypes = new ADBDatatype[initialColumnCount];
        objectStore = new Object[initialColumnCount];
        registerStore = new long[initialColumnCount * 2];
    }

    void reset() {
        Arrays.fill(columnTypes, ADBDatatype.MISSING);
        Arrays.fill(registerStore, 0);
        Arrays.fill(objectStore, null);
    }

    private void setColumnType(int columnIndex, ADBDatatype columnType) {
        columnTypes[columnIndex] = columnType;
    }

    ADBDatatype getColumnType(int columnIndex) {
        return columnTypes[columnIndex];
    }

    void putColumn(int columnIndex, char[] textChars, int textOffset, int textLength) throws SQLException {
        byte valueTypeTag = parseTypeTag(textChars, textOffset, textLength);
        ADBDatatype valueType = ADBDatatype.findByTypeTag(valueTypeTag);
        if (valueType == null) {
            throw getErrorReporter().errorUnexpectedType(valueTypeTag);
        }

        int nonTaggedOffset = textOffset + parsedLength;
        int nonTaggedLength = textLength - parsedLength;
        int nonTaggedEnd = nonTaggedOffset + nonTaggedLength; // = textOffset + textLength

        setColumnType(columnIndex, valueType);

        // NULL, BOOLEAN, BIGINT shouldn't normally happen. only handle here for completeness

        switch (valueType) {
            case MISSING:
            case NULL:
                // no content
                break;
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME:
            case DATETIME:
            case YEARMONTHDURATION:
            case DAYTIMEDURATION:
                long r0 = parseInt64(textChars, nonTaggedOffset, nonTaggedEnd);
                setColumnRegisters(columnIndex, r0, 0);
                break;
            case STRING:
                objectStore[columnIndex] = new String(textChars, nonTaggedOffset, nonTaggedLength);
                break;
            case DURATION:
                int delimiterOffset = indexOf(ADBProtocol.TEXT_DELIMITER, textChars, nonTaggedOffset, nonTaggedEnd);
                if (delimiterOffset < 0 || delimiterOffset == nonTaggedEnd - 1) {
                    throw getErrorReporter().errorInProtocol();
                }
                r0 = parseInt64(textChars, nonTaggedOffset, delimiterOffset);
                long r1 = parseInt64(textChars, delimiterOffset + 1, nonTaggedEnd);
                setColumnRegisters(columnIndex, r0, r1);
                break;
            case UUID:
                // TODO: better encoding as 2 longs?
                objectStore[columnIndex] = UUID.fromString(new String(textChars, nonTaggedOffset, nonTaggedLength));
                break;
            case OBJECT:
            case ARRAY:
            case MULTISET:
                // Unexpected (shouldn't be called)
                throw new IllegalArgumentException(String.valueOf(valueType));
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    void putNullColumn(int columnIndex) {
        setColumnType(columnIndex, ADBDatatype.NULL);
    }

    void putBooleanColumn(int columnIndex, boolean value) {
        setColumnType(columnIndex, ADBDatatype.BOOLEAN);
        setColumnRegisters(columnIndex, value ? 1 : 0, 0);
    }

    void putInt64Column(int columnIndex, long value) {
        setColumnType(columnIndex, ADBDatatype.BIGINT);
        setColumnRegisters(columnIndex, value, 0);
    }

    void putArrayColumn(int columnIndex, List<?> value) {
        setColumnType(columnIndex, ADBDatatype.ARRAY);
        objectStore[columnIndex] = Objects.requireNonNull(value);
    }

    void putRecordColumn(int columnIndex, Map<?, ?> value) {
        setColumnType(columnIndex, ADBDatatype.OBJECT);
        objectStore[columnIndex] = Objects.requireNonNull(value);
    }

    private void setColumnRegisters(int columnIndex, long r0, long r1) {
        int registerPos = columnIndex * 2;
        registerStore[registerPos] = r0;
        registerStore[++registerPos] = r1;
    }

    private long getColumnRegister(int columnIndex, int registerIndex) {
        int registerPos = columnIndex * 2;
        switch (registerIndex) {
            case 0:
                break;
            case 1:
                registerPos++;
                break;
            default:
                throw new IllegalArgumentException();
        }
        return registerStore[registerPos];
    }

    private boolean getColumnRegisterAsBoolean(int columnIndex, int registerIndex) {
        return getColumnRegister(columnIndex, registerIndex) != 0;
    }

    private byte getColumnRegisterAsByte(int columnIndex, int registerIndex) {
        return (byte) getColumnRegister(columnIndex, registerIndex);
    }

    private short getColumnRegisterAsShort(int columnIndex, int registerIndex) {
        return (short) getColumnRegister(columnIndex, registerIndex);
    }

    private int getColumnRegisterAsInt(int columnIndex, int registerIndex) {
        return (int) getColumnRegister(columnIndex, registerIndex);
    }

    private float getColumnRegisterAsFloat(int columnIndex, int registerIndex) {
        return Float.intBitsToFloat(getColumnRegisterAsFloatBits(columnIndex, registerIndex));
    }

    private boolean isColumnRegisterZeroOrNanFloat(int columnIndex, int registerIndex) {
        int bits = getColumnRegisterAsFloatBits(columnIndex, registerIndex);
        return bits == FLOAT_POSITIVE_ZERO_BITS || bits == FLOAT_NEGATIVE_ZERO_BITS || bits == FLOAT_NAN_BITS;
    }

    private int getColumnRegisterAsFloatBits(int columnIndex, int registerIndex) {
        return getColumnRegisterAsInt(columnIndex, registerIndex);
    }

    private double getColumnRegisterAsDouble(int columnIndex, int registerIndex) {
        return Double.longBitsToDouble(getColumnRegisterAsDoubleBits(columnIndex, registerIndex));
    }

    private boolean isColumnRegisterZeroOrNanDouble(int columnIndex, int registerIndex) {
        long bits = getColumnRegisterAsDoubleBits(columnIndex, registerIndex);
        return bits == DOUBLE_POSITIVE_ZERO_BITS || bits == DOUBLE_NEGATIVE_ZERO_BITS || bits == DOUBLE_NAN_BITS;
    }

    private long getColumnRegisterAsDoubleBits(int columnIndex, int registerIndex) {
        return getColumnRegister(columnIndex, registerIndex);
    }

    private Period getColumnRegisterAsPeriod(int columnIndex, int registerIndex) {
        return Period.ofMonths((int) getColumnRegister(columnIndex, registerIndex));
    }

    private Duration getColumnRegisterAsDuration(int columnIndex, int registerIndex) {
        return Duration.ofMillis((int) getColumnRegister(columnIndex, registerIndex));
    }

    private Number getNumberFromObjectStore(int columnIndex) {
        Object o = objectStore[columnIndex];
        if (o != null) {
            return (Number) o;
        }
        Number n;
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case TINYINT:
                n = getColumnRegisterAsByte(columnIndex, 0);
                break;
            case SMALLINT:
                n = getColumnRegisterAsShort(columnIndex, 0);
                break;
            case INTEGER:
                n = getColumnRegisterAsInt(columnIndex, 0);
                break;
            case BIGINT:
                n = getColumnRegister(columnIndex, 0);
                break;
            case FLOAT:
                n = getColumnRegisterAsFloat(columnIndex, 0);
                break;
            case DOUBLE:
                n = getColumnRegisterAsDouble(columnIndex, 0);
                break;
            default:
                throw new IllegalArgumentException(String.valueOf(valueType));
        }
        objectStore[columnIndex] = n;
        return n;
    }

    private String getStringFromObjectStore(int columnIndex) {
        return (String) objectStore[columnIndex];
    }

    private UUID getUUIDFromObjectStore(int columnIndex) {
        return (UUID) objectStore[columnIndex];
    }

    private Period getPeriodFromObjectStore(int columnIndex) {
        Object o = objectStore[columnIndex];
        if (o != null) {
            return (Period) o;
        }
        ADBDatatype valueType = getColumnType(columnIndex);
        if (valueType != ADBDatatype.YEARMONTHDURATION) {
            throw new IllegalArgumentException(String.valueOf(valueType));
        }
        Period v = getColumnRegisterAsPeriod(columnIndex, 0);
        objectStore[columnIndex] = v;
        return v;
    }

    private Duration getDurationFromObjectStore(int columnIndex) {
        Object o = objectStore[columnIndex];
        if (o != null) {
            return (Duration) o;
        }
        ADBDatatype valueType = getColumnType(columnIndex);
        if (valueType != ADBDatatype.DAYTIMEDURATION) {
            throw new IllegalArgumentException(String.valueOf(valueType));
        }
        Duration v = getColumnRegisterAsDuration(columnIndex, 0);
        objectStore[columnIndex] = v;
        return v;
    }

    private String getISODurationStringFromObjectStore(int columnIndex) {
        Object o = objectStore[columnIndex];
        if (o != null) {
            return (String) o;
        }
        ADBDatatype valueType = getColumnType(columnIndex);
        if (valueType != ADBDatatype.DURATION) {
            throw new IllegalArgumentException(String.valueOf(valueType));
        }
        String v = getColumnRegisterAsPeriod(columnIndex, 0).toString()
                + getColumnRegisterAsDuration(columnIndex, 1).toString().substring(1);
        objectStore[columnIndex] = v;
        return v;
    }

    boolean getBoolean(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return false;
            case BOOLEAN:
                return getColumnRegisterAsBoolean(columnIndex, 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return getColumnRegister(columnIndex, 0) != 0;
            case FLOAT:
                return !isColumnRegisterZeroOrNanFloat(columnIndex, 0);
            case DOUBLE:
                return !isColumnRegisterZeroOrNanDouble(columnIndex, 0);
            case STRING:
                return Boolean.parseBoolean(getStringFromObjectStore(columnIndex));
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    byte getByte(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return 0;
            case BOOLEAN:
                return (byte) (getColumnRegisterAsBoolean(columnIndex, 0) ? 1 : 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return getColumnRegisterAsByte(columnIndex, 0);
            case FLOAT:
                return (byte) getColumnRegisterAsFloat(columnIndex, 0);
            case DOUBLE:
                return (byte) getColumnRegisterAsDouble(columnIndex, 0);
            case STRING:
                return (byte) parseInt64(getStringFromObjectStore(columnIndex));
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    short getShort(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return 0;
            case BOOLEAN:
                return (short) (getColumnRegisterAsBoolean(columnIndex, 0) ? 1 : 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return getColumnRegisterAsShort(columnIndex, 0);
            case FLOAT:
                return (short) getColumnRegisterAsFloat(columnIndex, 0);
            case DOUBLE:
                return (short) getColumnRegisterAsDouble(columnIndex, 0);
            case STRING:
                return (short) parseInt64(getStringFromObjectStore(columnIndex));
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    int getInt(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return 0;
            case BOOLEAN:
                return getColumnRegisterAsBoolean(columnIndex, 0) ? 1 : 0;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DATE:
            case TIME:
            case YEARMONTHDURATION:
                return getColumnRegisterAsInt(columnIndex, 0);
            case FLOAT:
                return (int) getColumnRegisterAsFloat(columnIndex, 0);
            case DOUBLE:
                return (int) getColumnRegisterAsDouble(columnIndex, 0);
            case STRING:
                return (int) parseInt64(getStringFromObjectStore(columnIndex));
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    long getLong(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return 0;
            case BOOLEAN:
                return getColumnRegisterAsBoolean(columnIndex, 0) ? 1 : 0;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DATE:
            case TIME:
            case DATETIME:
            case YEARMONTHDURATION:
            case DAYTIMEDURATION:
                return getColumnRegister(columnIndex, 0);
            case FLOAT:
                return (long) getColumnRegisterAsFloat(columnIndex, 0);
            case DOUBLE:
                return (long) getColumnRegisterAsDouble(columnIndex, 0);
            case STRING:
                return parseInt64(getStringFromObjectStore(columnIndex));
            default:
                // TODO:support temporal types?
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    float getFloat(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return 0;
            case BOOLEAN:
                return getColumnRegisterAsBoolean(columnIndex, 0) ? 1f : 0f;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return getColumnRegister(columnIndex, 0);
            case FLOAT:
                return getColumnRegisterAsFloat(columnIndex, 0);
            case DOUBLE:
                return (float) getColumnRegisterAsDouble(columnIndex, 0);
            case STRING:
                try {
                    return Float.parseFloat(getStringFromObjectStore(columnIndex));
                } catch (NumberFormatException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    double getDouble(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return 0;
            case BOOLEAN:
                return getColumnRegisterAsBoolean(columnIndex, 0) ? 1d : 0d;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return getColumnRegister(columnIndex, 0);
            case FLOAT:
                return getColumnRegisterAsFloat(columnIndex, 0);
            case DOUBLE:
                return getColumnRegisterAsDouble(columnIndex, 0);
            case STRING:
                try {
                    return Double.parseDouble(getStringFromObjectStore(columnIndex));
                } catch (NumberFormatException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(columnIndex, false, 0);
    }

    @SuppressWarnings("UnpredictableBigDecimalConstructorCall")
    BigDecimal getBigDecimal(int columnIndex, boolean setScale, int scale) throws SQLException {
        BigDecimal dec;
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case BOOLEAN:
                dec = getColumnRegisterAsBoolean(columnIndex, 0) ? BigDecimal.ONE : BigDecimal.ZERO;
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DATE:
            case TIME:
            case DATETIME:
            case YEARMONTHDURATION:
            case DAYTIMEDURATION:
                dec = BigDecimal.valueOf(getColumnRegister(columnIndex, 0));
                break;
            case FLOAT:
                try {
                    dec = new BigDecimal(getColumnRegisterAsFloat(columnIndex, 0));
                } catch (NumberFormatException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
                break;
            case DOUBLE:
                try {
                    dec = new BigDecimal(getColumnRegisterAsDouble(columnIndex, 0));
                } catch (NumberFormatException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
                break;
            case STRING:
                try {
                    dec = new BigDecimal(getStringFromObjectStore(columnIndex));
                } catch (NumberFormatException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
                break;
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }

        return setScale ? dec.setScale(scale, RoundingMode.DOWN) : dec;
    }

    private Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, null);
    }

    Date getDate(int columnIndex, Calendar cal) throws SQLException {
        // TODO:cal is not used
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case DATE:
                return toDateFromDateChronon(getColumnRegister(columnIndex, 0));
            case DATETIME:
                return toDateFromDatetimeChronon(getColumnRegister(columnIndex, 0));
            case STRING:
                try {
                    LocalDate d = LocalDate.parse(getStringFromObjectStore(columnIndex)); // TODO:review
                    return new Date(d.getYear() - 1900, d.getMonthValue() - 1, d.getDayOfMonth());
                } catch (DateTimeParseException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    LocalDate getLocalDate(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case DATE:
                return toLocalDateFromDateChronon(getColumnRegister(columnIndex, 0));
            case DATETIME:
                return toLocalDateFromDatetimeChronon(getColumnRegister(columnIndex, 0));
            case STRING:
                try {
                    return LocalDate.parse(getStringFromObjectStore(columnIndex)); // TODO:review
                } catch (DateTimeParseException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    private Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, null);
    }

    Time getTime(int columnIndex, Calendar cal) throws SQLException {
        // TODO:cal not used
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case TIME:
                return toTimeFromTimeChronon(getColumnRegister(columnIndex, 0));
            case DATETIME:
                return toTimeFromDatetimeChronon(getColumnRegister(columnIndex, 0));
            case STRING:
                try {
                    LocalTime t = LocalTime.parse(getStringFromObjectStore(columnIndex)); // TODO:review
                    return toTimeFromTimeChronon(TimeUnit.NANOSECONDS.toMillis(t.toNanoOfDay()));
                } catch (DateTimeParseException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    LocalTime getLocalTime(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case TIME:
                return toLocalTimeFromTimeChronon(getColumnRegister(columnIndex, 0));
            case DATETIME:
                return toLocalTimeFromDatetimeChronon(getColumnRegister(columnIndex, 0));
            case STRING:
                try {
                    return LocalTime.parse(getStringFromObjectStore(columnIndex)); // TODO:review
                } catch (DateTimeParseException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    private Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, null);
    }

    Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        //TODO:FIXME:CAL NOT USED
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case DATE:
                return toTimestampFromDateChronon(getColumnRegister(columnIndex, 0));
            case DATETIME:
                return toTimestampFromDatetimeChronon(getColumnRegister(columnIndex, 0));
            case STRING:
                try {
                    Instant i = Instant.parse(getStringFromObjectStore(columnIndex));
                    long millis0 = TimeUnit.SECONDS.toMillis(i.getEpochSecond());
                    long millis1 = TimeUnit.NANOSECONDS.toMillis(i.getNano());
                    return new Timestamp(millis0 + millis1);
                } catch (DateTimeParseException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    Instant getInstant(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case DATE:
                return toInstantFromDateChronon(getColumnRegister(columnIndex, 0));
            case DATETIME:
                return toInstantFromDatetimeChronon(getColumnRegister(columnIndex, 0));
            case STRING:
                try {
                    return Instant.parse(getStringFromObjectStore(columnIndex)); // TODO:review
                } catch (DateTimeParseException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    Period getPeriod(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case YEARMONTHDURATION:
                return getPeriodFromObjectStore(columnIndex);
            case DURATION:
                return getColumnRegisterAsPeriod(columnIndex, 0);
            case STRING:
                try {
                    return Period.parse(getStringFromObjectStore(columnIndex)); // TODO:review
                } catch (DateTimeParseException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    Duration getDuration(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case DAYTIMEDURATION:
                return getDurationFromObjectStore(columnIndex);
            case DURATION:
                return getColumnRegisterAsDuration(columnIndex, 1);
            case STRING:
                try {
                    return Duration.parse(getStringFromObjectStore(columnIndex)); // TODO:review
                } catch (DateTimeParseException e) {
                    throw getErrorReporter().errorInvalidValueOfType(valueType);
                }
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    byte[] getBinary(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case STRING:
                return getStringFromObjectStore(columnIndex).getBytes(StandardCharsets.UTF_8);
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    UUID getUUID(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case UUID:
                return getUUIDFromObjectStore(columnIndex);
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    String getString(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case BOOLEAN:
                return Boolean.toString(getColumnRegisterAsBoolean(columnIndex, 0));
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return Long.toString(getColumnRegister(columnIndex, 0));
            case FLOAT:
                return Float.toString(getColumnRegisterAsFloat(columnIndex, 0));
            case DOUBLE:
                return Double.toString(getColumnRegisterAsDouble(columnIndex, 0));
            case DATE:
                return toLocalDateFromDateChronon(getColumnRegister(columnIndex, 0)).toString(); // TODO:review
            case TIME:
                return toLocalTimeFromTimeChronon(getColumnRegister(columnIndex, 0)).toString(); // TODO:review
            case DATETIME:
                return toInstantFromDatetimeChronon(getColumnRegister(columnIndex, 0)).toString(); // TODO:review
            case YEARMONTHDURATION:
                return getPeriodFromObjectStore(columnIndex).toString(); // TODO:review
            case DAYTIMEDURATION:
                return getDurationFromObjectStore(columnIndex).toString(); // TODO:review
            case DURATION:
                return getISODurationStringFromObjectStore(columnIndex); // TODO:review
            case STRING:
                return getStringFromObjectStore(columnIndex);
            case UUID:
                return getUUIDFromObjectStore(columnIndex).toString();
            case OBJECT:
            case ARRAY:
                return printAsJson(objectStore[columnIndex]);
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    Reader getCharacterStream(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case STRING:
                return new StringReader(getStringFromObjectStore(columnIndex));
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    InputStream getInputStream(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case STRING:
                return new ByteArrayInputStream(getStringFromObjectStore(columnIndex).getBytes(StandardCharsets.UTF_8));
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    Object getObject(int columnIndex) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            case BOOLEAN:
                return getColumnRegisterAsBoolean(columnIndex, 0);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return getNumberFromObjectStore(columnIndex);
            case DATE:
                return toDateFromDateChronon(getColumnRegister(columnIndex, 0));
            case TIME:
                return toTimeFromTimeChronon(getColumnRegister(columnIndex, 0));
            case DATETIME:
                return toTimestampFromDatetimeChronon(getColumnRegister(columnIndex, 0));
            case YEARMONTHDURATION:
                return getPeriodFromObjectStore(columnIndex);
            case DAYTIMEDURATION:
                return getDurationFromObjectStore(columnIndex);
            case DURATION:
                return getISODurationStringFromObjectStore(columnIndex);
            case STRING:
                return getStringFromObjectStore(columnIndex);
            case UUID:
                return getUUIDFromObjectStore(columnIndex);
            case OBJECT:
            case ARRAY:
                return objectStore[columnIndex]; // TODO:how to make immutable?
            default:
                throw getErrorReporter().errorUnexpectedType(valueType);
        }
    }

    <T> T getObject(int columnIndex, Class<T> targetType) throws SQLException {
        ADBDatatype valueType = getColumnType(columnIndex);
        switch (valueType) {
            case MISSING:
            case NULL:
                return null;
            default:
                GetObjectFunction getter = OBJECT_ACCESSORS_ATOMIC.get(targetType);
                Object v;
                if (getter != null) {
                    v = getter.getObject(this, columnIndex);
                } else if (GET_OBJECT_NON_ATOMIC.contains(targetType)) {
                    v = getObject(columnIndex);
                } else {
                    throw getErrorReporter().errorUnexpectedType(targetType);
                }
                return targetType.cast(v);
        }
    }

    interface GetObjectFunction {
        Object getObject(ADBRowStore rowStore, int columnIndex) throws SQLException;
    }

    private static Map<Class<?>, GetObjectFunction> createAtomicObjectAccessorMap() {
        Map<Class<?>, GetObjectFunction> map = new HashMap<>();
        map.put(Boolean.TYPE, ADBRowStore::getBoolean);
        map.put(Boolean.class, ADBRowStore::getBoolean);
        map.put(Byte.TYPE, ADBRowStore::getByte);
        map.put(Byte.class, ADBRowStore::getByte);
        map.put(Short.TYPE, ADBRowStore::getShort);
        map.put(Short.class, ADBRowStore::getShort);
        map.put(Integer.TYPE, ADBRowStore::getInt);
        map.put(Integer.class, ADBRowStore::getInt);
        map.put(Long.TYPE, ADBRowStore::getLong);
        map.put(Long.class, ADBRowStore::getLong);
        map.put(Float.TYPE, ADBRowStore::getFloat);
        map.put(Float.class, ADBRowStore::getFloat);
        map.put(Double.TYPE, ADBRowStore::getDouble);
        map.put(Double.class, ADBRowStore::getDouble);
        map.put(BigDecimal.class, ADBRowStore::getBigDecimal);
        map.put(Date.class, ADBRowStore::getDate);
        map.put(LocalDate.class, ADBRowStore::getLocalDate);
        map.put(Time.class, ADBRowStore::getTime);
        map.put(LocalTime.class, ADBRowStore::getLocalTime);
        map.put(Timestamp.class, ADBRowStore::getTimestamp);
        map.put(Instant.class, ADBRowStore::getInstant);
        map.put(Period.class, ADBRowStore::getPeriod);
        map.put(Duration.class, ADBRowStore::getDuration);
        map.put(UUID.class, ADBRowStore::getUUID);
        map.put(String.class, ADBRowStore::getString);
        return map;
    }

    private Date toDateFromDateChronon(long dateChrononInDays) {
        return new Date(TimeUnit.DAYS.toMillis(dateChrononInDays));
    }

    private Date toDateFromDatetimeChronon(long datetimeChrononInMillis) {
        return new Date(datetimeChrononInMillis);
    }

    private LocalDate toLocalDateFromDateChronon(long dateChrononInDays) {
        return LocalDate.ofEpochDay(dateChrononInDays);
    }

    private LocalDate toLocalDateFromDatetimeChronon(long datetimeChrononInMillis) {
        return LocalDate.ofEpochDay(TimeUnit.MILLISECONDS.toDays(datetimeChrononInMillis));
    }

    private Time toTimeFromTimeChronon(long timeChrononInMillis) {
        long datetimeChrononInMillis = getCurrentDateChrononInMillis() + timeChrononInMillis;
        return new Time(datetimeChrononInMillis);
    }

    private Time toTimeFromDatetimeChronon(long datetimeChrononInMillis) {
        return new Time(datetimeChrononInMillis);
    }

    private LocalTime toLocalTimeFromTimeChronon(long timeChrononInMillis) {
        return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(timeChrononInMillis));
    }

    private LocalTime toLocalTimeFromDatetimeChronon(long datetimeChrononInMillis) {
        return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(datetimeChrononInMillis));
    }

    private Timestamp toTimestampFromDatetimeChronon(long datetimeChrononInMillis) {
        return new Timestamp(datetimeChrononInMillis);
    }

    private Timestamp toTimestampFromDateChronon(long dateChrononInDays) {
        return new Timestamp(TimeUnit.DAYS.toMillis(dateChrononInDays));
    }

    private Instant toInstantFromDatetimeChronon(long datetimeChrononInMillis) {
        return Instant.ofEpochMilli(datetimeChrononInMillis);
    }

    private Instant toInstantFromDateChronon(long dateChrononInDays) {
        return Instant.ofEpochMilli(TimeUnit.DAYS.toMillis(dateChrononInDays));
    }

    private long getCurrentDateChrononInMillis() {
        if (currentDateChronon == 0) {
            long chrononOfDay = TimeUnit.DAYS.toMillis(1);
            currentDateChronon = System.currentTimeMillis() / chrononOfDay * chrononOfDay;
        }
        return currentDateChronon;
    }

    private String printAsJson(Object value) throws SQLException {
        if (jsonGenBuffer == null) {
            jsonGenBuffer = new StringWriter();
            try {
                //TODO:FIXME:need to configure generator to print java.sql.Date/Times properly
                jsonGen = resultSet.metadata.statement.connection.protocol.driverContext.genericObjectWriter
                        .createGenerator(jsonGenBuffer);
            } catch (IOException e) {
                throw getErrorReporter().errorInResultHandling(e);
            }
        }
        try {
            jsonGen.writeObject(value);
            jsonGen.flush();
            return jsonGenBuffer.getBuffer().toString();
        } catch (IOException e) {
            throw getErrorReporter().errorInResultHandling(e);
        } finally {
            jsonGenBuffer.getBuffer().setLength(0);
        }
    }

    ObjectReader createComplexColumnObjectReader(ObjectReader templateReader) {
        return templateReader.withAttribute(ROW_STORE_ATTR_NAME, this);
    }

    static void configureDeserialization(ObjectMapper objectMapper, SimpleModule serdeModule) {
        objectMapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
        serdeModule.setDeserializerModifier(createADMFormatDeserializerModifier());
    }

    private static BeanDeserializerModifier createADMFormatDeserializerModifier() {
        return new BeanDeserializerModifier() {
            @Override
            public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, BeanDescription beanDesc,
                    JsonDeserializer<?> deserializer) {
                if (String.class.equals(beanDesc.getClassInfo().getAnnotated())) {
                    ADBRowStore rowStore = (ADBRowStore) config.getAttributes().getAttribute(ROW_STORE_ATTR_NAME);
                    return rowStore.createADMFormatStringDeserializer();
                } else {
                    return deserializer;
                }
            }
        };
    }

    private JsonDeserializer<?> createADMFormatStringDeserializer() {
        return new JsonDeserializer<Object>() {
            @Override
            public Object deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
                if (!parser.hasToken(JsonToken.VALUE_STRING)) {
                    throw new IOException("Unexpected token");
                }
                try {
                    ADBRowStore.this.reset();
                    ADBRowStore.this.putColumn(0, parser.getTextCharacters(), parser.getTextOffset(),
                            parser.getTextLength());
                    return ADBRowStore.this.getObject(0);
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    @FunctionalInterface
    public interface ICharAccessor<T> {
        char charAt(T input, int index);
    }

    private long parseInt64(CharSequence buffer) throws SQLException {
        return parseInt64(buffer, 0, buffer.length(), CharSequence::charAt);
    }

    private long parseInt64(char[] buffer, int begin, int end) throws SQLException {
        return parseInt64(buffer, begin, end, (input, index) -> input[index]);
    }

    private <T> long parseInt64(T buffer, int begin, int end, ICharAccessor<T> charAccessor) throws SQLException {
        if (end < begin) {
            throw new IllegalArgumentException();
        }
        boolean positive = true;
        long value = 0;
        int offset = begin;
        char c = charAccessor.charAt(buffer, offset);
        if (c == '+') {
            offset++;
        } else if (c == '-') {
            offset++;
            positive = false;
        }
        try {
            for (; offset < end; offset++) {
                c = charAccessor.charAt(buffer, offset);
                if (c >= '0' && c <= '9') {
                    value = Math.addExact(Math.multiplyExact(value, 10L), '0' - c);
                } else {
                    throw getErrorReporter().errorInProtocol(String.valueOf(c));
                }
            }
            if (positive) {
                value = Math.multiplyExact(value, -1L);
            }
            return value;
        } catch (ArithmeticException e) {
            throw getErrorReporter().errorInProtocol();
        }
    }

    private byte parseTypeTag(char[] textChars, int textOffset, int textLength) throws SQLException {
        if (textLength == 0) {
            // empty string
            parsedLength = 0;
            return ADBDatatype.STRING.getTypeTag();
        }
        if (textChars[textOffset] == ADBProtocol.TEXT_DELIMITER) {
            // any string
            parsedLength = 1;
            return ADBDatatype.STRING.getTypeTag();
        }
        // any type
        int typeTagLength = 2;
        if (textLength < typeTagLength) {
            throw getErrorReporter().errorInProtocol();
        }
        byte parsedTypeTag = getByteFromValidHexChars(textChars[textOffset], textChars[textOffset + 1]);
        if (parsedTypeTag == ADBDatatype.MISSING.getTypeTag() || parsedTypeTag == ADBDatatype.NULL.getTypeTag()) {
            parsedLength = typeTagLength;
            return parsedTypeTag;
        }
        int delimiterLength = 1;
        if (textLength < typeTagLength + delimiterLength) {
            throw getErrorReporter().errorInProtocol();
        }
        if (textChars[textOffset + typeTagLength] != ADBProtocol.TEXT_DELIMITER) {
            throw getErrorReporter().errorInProtocol();
        }
        parsedLength = typeTagLength + delimiterLength;
        return parsedTypeTag;
    }

    private byte getByteFromValidHexChars(char c0, char c1) throws SQLException {
        return (byte) ((getValueFromValidHexChar(c0) << 4) + getValueFromValidHexChar(c1));
    }

    private int getValueFromValidHexChar(char c) throws SQLException {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return 10 + c - 'a';
        }
        if (c >= 'A' && c <= 'F') {
            return 10 + c - 'A';
        }
        throw getErrorReporter().errorInProtocol(String.valueOf(c));
    }

    private static int indexOf(char c, char[] array, int begin, int end) {
        for (int i = begin; i < end; i++) {
            if (array[i] == c) {
                return i;
            }
        }
        return -1;
    }

    private ADBErrorReporter getErrorReporter() {
        return resultSet.getErrorReporter();
    }
}
