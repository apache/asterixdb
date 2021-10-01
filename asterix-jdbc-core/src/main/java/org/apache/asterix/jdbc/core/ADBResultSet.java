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
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectReader;

final class ADBResultSet extends ADBWrapperSupport implements java.sql.ResultSet {

    static final int RESULT_SET_HOLDABILITY = HOLD_CURSORS_OVER_COMMIT;

    static final int ST_BEFORE_FIRST = 0;
    static final int ST_NEXT = 1;
    static final int ST_AFTER_LAST = 2;

    // lifecycle
    final AtomicBoolean closed = new AtomicBoolean(false);

    // metadata
    final ADBResultSetMetaData metadata;

    // navigation
    final JsonParser rowParser;
    final boolean rowParserOwnsResources;
    final long maxRows;

    int state;
    long rowNumber;
    ADBRowStore rowStore;
    ObjectReader complexColumnReader;
    int columnIndexOfLatestGet;

    // Lifecycle

    ADBResultSet(ADBResultSetMetaData metadata, JsonParser rowParser, boolean rowParserOwnsResources, long maxRows) {
        this.metadata = Objects.requireNonNull(metadata);
        this.rowParser = Objects.requireNonNull(rowParser);
        this.rowParserOwnsResources = rowParserOwnsResources;
        this.maxRows = maxRows;
        this.state = ST_BEFORE_FIRST;
    }

    @Override
    public void close() throws SQLException {
        closeImpl(true);
    }

    void closeImpl(boolean notifyStatement) throws SQLException {
        boolean wasClosed = closed.getAndSet(true);
        if (wasClosed) {
            return;
        }
        try {
            rowParser.close();
        } catch (IOException e) {
            throw getErrorReporter().errorClosingResource(e);
        } finally {
            if (notifyStatement) {
                metadata.statement.deregisterResultSet(this);
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw getErrorReporter().errorObjectClosed(ResultSet.class);
        }
    }

    // Metadata

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return metadata;
    }

    // Navigation

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        try {
            switch (state) {
                case ST_BEFORE_FIRST:
                    JsonToken token = rowParser.hasCurrentToken() ? rowParser.currentToken() : rowParser.nextToken();
                    if (token != JsonToken.START_ARRAY) {
                        throw getErrorReporter().errorInProtocol(String.valueOf(token));
                    }
                    initRowStore();
                    state = ST_NEXT;
                    // fall thru to ST_NEXT
                case ST_NEXT:
                    token = rowParser.nextToken();
                    switch (token) {
                        case START_OBJECT:
                            if (maxRows > 0 && rowNumber == maxRows) {
                                state = ST_AFTER_LAST;
                                return false;
                            } else {
                                readRow();
                                rowNumber++;
                                return true;
                            }
                        case END_ARRAY:
                            state = ST_AFTER_LAST;
                            return false;
                        default:
                            throw getErrorReporter().errorInProtocol(String.valueOf(token));
                    }
                case ST_AFTER_LAST:
                    return false;
                default:
                    throw new IllegalStateException(String.valueOf(state));
            }
        } catch (JsonProcessingException e) {
            throw getErrorReporter().errorInProtocol(e);
        } catch (IOException e) {
            throw getErrorReporter().errorInConnection(e);
        }
    }

    private void initRowStore() {
        rowStore = createRowStore(metadata.getColumnCount());
    }

    protected ADBRowStore createRowStore(int columnCount) {
        return new ADBRowStore(this, columnCount);
    }

    private void readRow() throws SQLException {
        rowStore.reset();
        columnIndexOfLatestGet = -1;
        if (rowParser.currentToken() != JsonToken.START_OBJECT) {
            throw new IllegalStateException();
        }
        try {
            while (rowParser.nextToken() == JsonToken.FIELD_NAME) {
                String fieldName = rowParser.getCurrentName();
                int columnIndex = metadata.findColumnIndexByName(fieldName);
                boolean isKnownColumn = columnIndex >= 0;
                ADBColumn column = isKnownColumn ? metadata.getColumnByIndex(columnIndex) : null;

                switch (rowParser.nextToken()) {
                    case VALUE_NULL:
                        if (isKnownColumn) {
                            typeCheck(column, ADBDatatype.NULL);
                            rowStore.putNullColumn(columnIndex);
                        }
                        break;
                    case VALUE_TRUE:
                        if (isKnownColumn) {
                            typeCheck(column, ADBDatatype.BOOLEAN);
                            rowStore.putBooleanColumn(columnIndex, true);
                        }
                        break;
                    case VALUE_FALSE:
                        if (isKnownColumn) {
                            typeCheck(column, ADBDatatype.BOOLEAN);
                            rowStore.putBooleanColumn(columnIndex, false);
                        }
                        break;
                    case VALUE_NUMBER_INT:
                        if (isKnownColumn) {
                            typeCheck(column, ADBDatatype.BIGINT);
                            long v = rowParser.getLongValue();
                            rowStore.putInt64Column(columnIndex, v);
                        }
                        break;
                    case VALUE_STRING:
                        if (isKnownColumn) {
                            typeCheck(column, ADBDatatype.STRING);
                            char[] textChars = rowParser.getTextCharacters();
                            int textOffset = rowParser.getTextOffset();
                            int textLength = rowParser.getTextLength();
                            rowStore.putColumn(columnIndex, textChars, textOffset, textLength);
                        }
                        break;
                    case START_OBJECT:
                        if (isKnownColumn) {
                            typeCheck(column, ADBDatatype.OBJECT);
                            Map<?, ?> valueMap = getComplexColumnReader().readValue(rowParser, Map.class);
                            rowStore.putRecordColumn(columnIndex, valueMap);
                        } else {
                            rowParser.skipChildren();
                        }
                        break;
                    case START_ARRAY:
                        if (isKnownColumn) {
                            typeCheck(column, ADBDatatype.ARRAY);
                            List<?> valueList = getComplexColumnReader().readValue(rowParser, List.class);
                            rowStore.putArrayColumn(columnIndex, valueList);
                        } else {
                            rowParser.skipChildren();
                        }
                        break;
                    default:
                        throw getErrorReporter().errorInProtocol(String.valueOf(rowParser.currentToken()));
                }
            }
        } catch (JsonProcessingException e) {
            throw getErrorReporter().errorInProtocol(e);
        } catch (IOException e) {
            throw getErrorReporter().errorInConnection(e);
        }
    }

    private void typeCheck(ADBColumn column, ADBDatatype parsedType) throws SQLException {
        ADBDatatype columnType = column.getType();

        boolean typeMatch;
        switch (parsedType) {
            case NULL:
                typeMatch = column.isOptional();
                break;
            case STRING:
                // special handling for parsed 'string' because it can contain any primitive type.
                // we only need to check that the expected type is not derived (i.e primitive/null/missing/any)
                typeMatch = !columnType.isDerived();
                break;
            case ARRAY:
                typeMatch = columnType == ADBDatatype.ANY || columnType.isList();
                break;
            case BOOLEAN:
            case BIGINT:
            case OBJECT:
                typeMatch = columnType == ADBDatatype.ANY || columnType == parsedType;
                break;
            default:
                // unexpected
                throw getErrorReporter().errorInProtocol(parsedType.toString());
        }
        if (!typeMatch) {
            throw getErrorReporter().errorUnexpectedColumnValue(parsedType, column.getName());
        }
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("FORWARD_ONLY");
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("FORWARD_ONLY");
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("FORWARD_ONLY");
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("FORWARD_ONLY");
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("FORWARD_ONLY");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("FORWARD_ONLY");
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("FORWARD_ONLY");
    }

    @Override
    public boolean isBeforeFirst() {
        return state == ST_BEFORE_FIRST;
    }

    @Override
    public boolean isAfterLast() {
        return state == ST_AFTER_LAST;
    }

    @Override
    public boolean isFirst() {
        return state == ST_NEXT && rowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "isLast");
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return (int) rowNumber;
    }

    private void checkCursorPosition() throws SQLException {
        if (state != ST_NEXT) {
            throw getErrorReporter().errorNoCurrentRow();
        }
    }

    private ObjectReader getComplexColumnReader() {
        if (complexColumnReader == null) {
            ADBDriverContext ctx = metadata.statement.connection.protocol.driverContext;
            ADBRowStore tmpStore = createRowStore(1);
            complexColumnReader = tmpStore.createComplexColumnObjectReader(ctx.admFormatObjectReader);
        }
        return complexColumnReader;
    }

    // Column accessors

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        int columnIndex = metadata.findColumnIndexByName(columnLabel);
        if (columnIndex < 0) {
            throw getErrorReporter().errorColumnNotFound(columnLabel);
        }
        return columnIndex + 1;
    }

    // Column accessors: basic types

    private int fetchColumnIndex(int columnNumber) throws SQLException {
        if (columnNumber < 1 || columnNumber > metadata.getColumnCount()) {
            throw getErrorReporter().errorColumnNotFound(String.valueOf(columnNumber));
        }
        return columnNumber - 1;
    }

    private int fetchColumnIndex(String columnLabel) throws SQLException {
        int columnIndex = metadata.findColumnIndexByName(columnLabel);
        if (columnIndex < 0) {
            throw getErrorReporter().errorColumnNotFound(columnLabel);
        }
        return columnIndex;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        checkCursorPosition();
        if (columnIndexOfLatestGet < 0) {
            return false;
        }
        ADBDatatype columnValueType = rowStore.getColumnType(columnIndexOfLatestGet);
        return columnValueType == ADBDatatype.NULL || columnValueType == ADBDatatype.MISSING;
    }

    @Override
    public boolean getBoolean(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getBooleanImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getBooleanImpl(fetchColumnIndex(columnLabel));
    }

    private boolean getBooleanImpl(int columnIndex) throws SQLException {
        boolean v = rowStore.getBoolean(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public byte getByte(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getByteImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getByteImpl(fetchColumnIndex(columnLabel));
    }

    private byte getByteImpl(int columnIndex) throws SQLException {
        byte v = rowStore.getByte(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public short getShort(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getShortImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getShortImpl(fetchColumnIndex(columnLabel));
    }

    private short getShortImpl(int columnIndex) throws SQLException {
        short v = rowStore.getShort(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public int getInt(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getIntImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getIntImpl(fetchColumnIndex(columnLabel));
    }

    private int getIntImpl(int columnIndex) throws SQLException {
        int v = rowStore.getInt(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public long getLong(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getLongImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getLongImpl(fetchColumnIndex(columnLabel));
    }

    private long getLongImpl(int columnIndex) throws SQLException {
        long v = rowStore.getLong(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public float getFloat(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getFloatImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getFloatImpl(fetchColumnIndex(columnLabel));
    }

    private float getFloatImpl(int columnIndex) throws SQLException {
        float v = rowStore.getFloat(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public double getDouble(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getDoubleImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getDoubleImpl(fetchColumnIndex(columnLabel));
    }

    private double getDoubleImpl(int columnIndex) throws SQLException {
        double v = rowStore.getDouble(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public BigDecimal getBigDecimal(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getBigDecimalImpl(fetchColumnIndex(columnNumber), false, -1);
    }

    @Override
    public BigDecimal getBigDecimal(int columnNumber, int scale) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getBigDecimalImpl(fetchColumnIndex(columnNumber), true, scale);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getBigDecimalImpl(fetchColumnIndex(columnLabel), false, -1);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getBigDecimalImpl(fetchColumnIndex(columnLabel), true, scale);
    }

    private BigDecimal getBigDecimalImpl(int columnIndex, boolean setScale, int scale) throws SQLException {
        BigDecimal v = rowStore.getBigDecimal(columnIndex, setScale, scale);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public Date getDate(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getDateImpl(fetchColumnIndex(columnNumber), null);
    }

    @Override
    public Date getDate(int columnNumber, Calendar cal) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getDateImpl(fetchColumnIndex(columnNumber), cal);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getDateImpl(fetchColumnIndex(columnLabel), null);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getDateImpl(fetchColumnIndex(columnLabel), cal);
    }

    private Date getDateImpl(int columnIndex, Calendar cal) throws SQLException {
        Date v = rowStore.getDate(columnIndex, cal);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public Time getTime(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getTimeImpl(fetchColumnIndex(columnNumber), null);
    }

    @Override
    public Time getTime(int columnNumber, Calendar cal) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getTimeImpl(fetchColumnIndex(columnNumber), cal);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getTimeImpl(fetchColumnIndex(columnLabel), null);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getTimeImpl(fetchColumnIndex(columnLabel), cal);
    }

    private Time getTimeImpl(int columnIndex, Calendar cal) throws SQLException {
        Time v = rowStore.getTime(columnIndex, cal);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public Timestamp getTimestamp(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getTimestampImpl(fetchColumnIndex(columnNumber), null);
    }

    @Override
    public Timestamp getTimestamp(int columnNumber, Calendar cal) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getTimestampImpl(fetchColumnIndex(columnNumber), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getTimestampImpl(fetchColumnIndex(columnLabel), null);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getTimestampImpl(fetchColumnIndex(columnLabel), cal);
    }

    private Timestamp getTimestampImpl(int columnIndex, Calendar cal) throws SQLException {
        Timestamp v = rowStore.getTimestamp(columnIndex, cal);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public String getString(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getStringImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getStringImpl(fetchColumnIndex(columnLabel));
    }

    @Override
    public String getNString(int columnNumber) throws SQLException {
        return getString(columnNumber);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    private String getStringImpl(int columnIndex) throws SQLException {
        String v = rowStore.getString(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public byte[] getBytes(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getBytesImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getBytesImpl(fetchColumnIndex(columnLabel));
    }

    private byte[] getBytesImpl(int columnIndex) throws SQLException {
        byte[] v = rowStore.getBinary(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    // Column accessor: Generic (getObject)

    @Override
    public Object getObject(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getObjectImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getObjectImpl(fetchColumnIndex(columnLabel));
    }

    @Override
    public <T> T getObject(int columnNumber, Class<T> type) throws SQLException {
        checkClosed();
        checkCursorPosition();
        if (type == null) {
            throw getErrorReporter().errorParameterValueNotSupported("type");
        }
        return getObjectImpl(fetchColumnIndex(columnNumber), type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        checkClosed();
        checkCursorPosition();
        if (type == null) {
            throw getErrorReporter().errorParameterValueNotSupported("type");
        }
        return getObjectImpl(fetchColumnIndex(columnLabel), type);
    }

    private Object getObjectImpl(int columnIndex) throws SQLException {
        ADBColumn column = metadata.getColumnByIndex(columnIndex);
        return getObjectImpl(columnIndex, column.getType().getJavaClass());
    }

    private <T> T getObjectImpl(int columnIndex, Class<T> type) throws SQLException {
        T v = rowStore.getObject(columnIndex, type);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getObject");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getObject");
    }

    // Column accessors: streams

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getBinaryStreamImpl(fetchColumnIndex(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getBinaryStreamImpl(fetchColumnIndex(columnLabel));
    }

    private InputStream getBinaryStreamImpl(int columnIndex) throws SQLException {
        InputStream v = rowStore.getInputStream(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public Reader getCharacterStream(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getCharacterStreamImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getCharacterStreamImpl(fetchColumnIndex(columnLabel));
    }

    private Reader getCharacterStreamImpl(int columnIndex) throws SQLException {
        Reader v = rowStore.getCharacterStream(columnIndex);
        columnIndexOfLatestGet = columnIndex;
        return v;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(columnLabel);
    }

    @Override
    public InputStream getAsciiStream(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getAsciiStreamImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getAsciiStreamImpl(fetchColumnIndex(columnLabel));
    }

    private InputStream getAsciiStreamImpl(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        return value != null ? new ByteArrayInputStream(value.getBytes(StandardCharsets.US_ASCII)) : null;
    }

    @Override
    public InputStream getUnicodeStream(int columnNumber) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getUnicodeStreamImpl(fetchColumnIndex(columnNumber));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        checkClosed();
        checkCursorPosition();
        return getUnicodeStreamImpl(fetchColumnIndex(columnLabel));
    }

    private InputStream getUnicodeStreamImpl(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        return value != null ? new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_16)) : null;
    }

    // Column accessors: unsupported

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getRef");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getRef");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getRowId");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getRowId");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getURL");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getURL");
    }

    // Column accessors: unsupported - LOB, Array, SQLXML

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getArray");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getArray");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getBlob");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getBlob");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getClob");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getClob");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getNClob");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getNClob");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "getSQLXML");
    }

    // Updates (unsupported)

    // Column setters

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateArray");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateArray");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateAsciiStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateAsciiStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateAsciiStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateAsciiStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateAsciiStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateAsciiStream");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBigDecimal");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBigDecimal");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBinaryStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBinaryStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBinaryStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBinaryStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBinaryStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBinaryStream");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBlob");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBlob");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBlob");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBoolean");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBoolean");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateByte");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateByte");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBytes");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateBytes");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateCharacterStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateCharacterStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateCharacterStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateCharacterStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateCharacterStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateCharacterStream");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateClob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateClob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateClob");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateDate");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateDate");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateDouble");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateDouble");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateFloat");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateFloat");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateInt");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateInt");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateLong");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateLong");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNCharacterStream");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNClob");
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNString");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNString");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNull");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateNull");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateObject");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateRef");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateRef");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateRowId");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateSQLXML");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateSQLXML");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateShort");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateShort");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateString");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateString");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateTime");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateTime");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateTimestamp");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateTimestamp");
    }

    // Update navigation and state (unsupported)

    @Override
    public void insertRow() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "insertRow");
    }

    @Override
    public void updateRow() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "updateRow");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "deleteRow");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "refreshRow");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "moveToCurrentRow");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "rowInserted");
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "rowUpdated");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "rowDeleted");

    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(ResultSet.class, "cancelRowUpdates");
    }

    // Errors and warnings

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    protected ADBErrorReporter getErrorReporter() {
        return metadata.getErrorReporter();
    }

    // Ownership

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return metadata.statement.getResultSetStatement(this);
    }

    // Cursor - related

    @Override
    public String getCursorName() throws SQLException {
        checkClosed();
        return "";
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return RESULT_SET_HOLDABILITY;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw getErrorReporter().errorParameterValueNotSupported("direction");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 1;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        // ignore value
    }
}
