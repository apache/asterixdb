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

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;

class ADBStatement extends ADBWrapperSupport implements java.sql.Statement {

    static final List<Class<?>> SET_OBJECT_NON_ATOMIC = Arrays.asList(Object[].class, Collection.class, Map.class);

    static final Map<Class<?>, AbstractValueSerializer> SERIALIZER_MAP = createSerializerMap();

    protected final ADBConnection connection;
    protected final String catalog;
    protected final String schema;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected volatile boolean closeOnCompletion;

    protected int queryTimeoutSeconds;
    protected long maxRows;

    // common result fields
    protected int updateCount = -1;
    protected List<ADBProtocol.QueryServiceResponse.Message> warnings;

    // executeQuery() result fields
    protected final ConcurrentLinkedQueue<ADBResultSet> resultSetsWithResources;
    protected final ConcurrentLinkedQueue<WeakReference<ADBResultSet>> resultSetsWithoutResources;

    // execute() result field
    protected ADBProtocol.QueryServiceResponse executeResponse;
    protected ADBResultSet executeResultSet;

    // Lifecycle

    ADBStatement(ADBConnection connection, String catalog, String schema) {
        this.connection = Objects.requireNonNull(connection);
        this.catalog = catalog;
        this.schema = schema;
        this.resultSetsWithResources = new ConcurrentLinkedQueue<>();
        this.resultSetsWithoutResources = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void close() throws SQLException {
        closeImpl(true, true);
    }

    void closeImpl(boolean closeResultSets, boolean notifyConnection) throws SQLException {
        boolean wasClosed = closed.getAndSet(true);
        if (wasClosed) {
            return;
        }
        try {
            if (closeResultSets) {
                closeRegisteredResultSets();
            }
        } finally {
            if (notifyConnection) {
                connection.deregisterStatement(this);
            }
        }
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return closeOnCompletion;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    protected void checkClosed() throws SQLException {
        if (isClosed()) {
            throw getErrorReporter().errorObjectClosed(Statement.class);
        }
    }

    // Execution

    @Override
    public ADBResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        return executeQueryImpl(sql, null);
    }

    protected ADBResultSet executeQueryImpl(String sql, List<?> args) throws SQLException {
        // note: we're not assigning executeResponse field at this method
        ADBProtocol.QueryServiceResponse response =
                connection.protocol.submitStatement(sql, args, true, false, queryTimeoutSeconds, catalog, schema);
        boolean isQuery = connection.protocol.isStatementCategory(response,
                ADBProtocol.QueryServiceResponse.StatementCategory.QUERY);
        if (!isQuery) {
            throw getErrorReporter().errorInvalidStatementCategory();
        }
        warnings = connection.protocol.getWarningIfExists(response);
        updateCount = -1;
        return fetchResultSet(response);
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        checkClosed();
        return executeUpdateImpl(sql, null);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        return executeUpdateImpl(sql, null);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "executeLargeUpdate");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "executeUpdate");
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "executeLargeUpdate");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "executeUpdate");
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "executeLargeUpdate");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "executeUpdate");
    }

    protected int executeUpdateImpl(String sql, List<Object> args) throws SQLException {
        ADBProtocol.QueryServiceResponse response =
                connection.protocol.submitStatement(sql, args, false, false, queryTimeoutSeconds, catalog, schema);
        boolean isQuery = connection.protocol.isStatementCategory(response,
                ADBProtocol.QueryServiceResponse.StatementCategory.QUERY);
        // TODO: remove result set on the server (both query and update returning cases)
        if (isQuery) {
            throw getErrorReporter().errorInvalidStatementCategory();
        }
        warnings = connection.protocol.getWarningIfExists(response);
        updateCount = connection.protocol.getUpdateCount(response);
        return updateCount;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        return executeImpl(sql, null);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "execute");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "execute");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "execute");
    }

    protected boolean executeImpl(String sql, List<Object> args) throws SQLException {
        ADBProtocol.QueryServiceResponse response =
                connection.protocol.submitStatement(sql, args, false, false, queryTimeoutSeconds, catalog, schema);
        warnings = connection.protocol.getWarningIfExists(response);
        boolean isQuery = connection.protocol.isStatementCategory(response,
                ADBProtocol.QueryServiceResponse.StatementCategory.QUERY);
        if (isQuery) {
            updateCount = -1;
            executeResponse = response;
            return true;
        } else {
            updateCount = connection.protocol.getUpdateCount(response);
            executeResponse = null;
            return false;
        }
    }

    @Override
    public void cancel() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "cancel");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeoutSeconds;
    }

    @Override
    public void setQueryTimeout(int timeoutSeconds) throws SQLException {
        checkClosed();
        if (timeoutSeconds < 0) {
            throw getErrorReporter().errorParameterValueNotSupported("timeoutSeconds");
        }
        queryTimeoutSeconds = timeoutSeconds;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
    }

    // Batch execution

    @Override
    public long[] executeLargeBatch() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "executeLargeBatch");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "executeBatch");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "addBatch");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "clearBatch");
    }

    // Result access

    @Override
    public ADBResultSet getResultSet() throws SQLException {
        checkClosed();
        ADBProtocol.QueryServiceResponse response = executeResponse;
        if (response == null) {
            return null;
        }
        ADBResultSet rs = fetchResultSet(response);
        executeResultSet = rs;
        executeResponse = null;
        return rs;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(Statement.CLOSE_ALL_RESULTS);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        ADBResultSet rs = executeResultSet;
        executeResultSet = null;
        if (rs != null && current != Statement.KEEP_CURRENT_RESULT) {
            rs.closeImpl(true);
        }
        return false;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return ADBResultSet.RESULT_SET_HOLDABILITY;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        return createEmptyResultSet();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        checkClosed();
        return updateCount;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return (int) getLargeUpdateCount();
    }

    // ResultSet lifecycle

    private ADBResultSet fetchResultSet(ADBProtocol.QueryServiceResponse execResponse) throws SQLException {
        List<ADBColumn> columns = connection.protocol.getColumns(execResponse);
        if (getLogger().isLoggable(Level.FINER)) {
            getLogger().log(Level.FINE, "result schema " + columns);
        }
        if (connection.protocol.isExplainOnly(execResponse)) {
            AbstractValueSerializer stringSer = getADMFormatSerializer(String.class);
            ArrayNode explainResult = connection.protocol.fetchExplainOnlyResult(execResponse, stringSer);
            return createSystemResultSet(columns, explainResult);
        } else {
            JsonParser rowParser = connection.protocol.fetchResult(execResponse);
            return createResultSetImpl(columns, rowParser, true, maxRows);
        }
    }

    protected ADBResultSet createSystemResultSet(List<ADBColumn> columns, ArrayNode values) {
        JsonParser rowParser = connection.protocol.createJsonParser(values);
        return createResultSetImpl(columns, rowParser, false, 0);
    }

    protected ADBResultSet createEmptyResultSet() {
        ArrayNode empty = (ArrayNode) connection.protocol.driverContext.genericObjectReader.createArrayNode();
        return createSystemResultSet(Collections.emptyList(), empty);
    }

    private ADBResultSet createResultSetImpl(List<ADBColumn> columns, JsonParser rowParser,
            boolean rowParserOwnsResources, long maxRows) {
        ADBResultSetMetaData metadata = new ADBResultSetMetaData(this, columns);
        ADBResultSet rs = new ADBResultSet(metadata, rowParser, rowParserOwnsResources, maxRows);
        registerResultSet(rs);
        return rs;
    }

    private void registerResultSet(ADBResultSet rs) {
        if (rs.rowParserOwnsResources) {
            resultSetsWithResources.add(rs);
        } else {
            resultSetsWithoutResources.removeIf(ADBStatement::isEmptyReference);
            resultSetsWithoutResources.add(new WeakReference<>(rs));
        }
    }

    protected void deregisterResultSet(ADBResultSet rs) {
        if (rs.rowParserOwnsResources) {
            resultSetsWithResources.remove(rs);
        } else {
            resultSetsWithoutResources.removeIf(ref -> {
                ADBResultSet refrs = ref.get();
                return refrs == null || refrs == rs;
            });
        }
        if (closeOnCompletion && resultSetsWithResources.isEmpty() && resultSetsWithoutResources.isEmpty()) {
            try {
                closeImpl(false, true);
            } catch (SQLException e) {
                // this exception shouldn't happen because there are no result sets to close
                if (getLogger().isLoggable(Level.FINE)) {
                    getLogger().log(Level.FINE, e.getMessage(), e);
                }
            }
        }
    }

    private void closeRegisteredResultSets() throws SQLException {
        SQLException err = null;
        try {
            closedRegisteredResultSetsImpl(resultSetsWithResources, Function.identity());
        } catch (SQLException e) {
            err = e;
        }
        try {
            closedRegisteredResultSetsImpl(resultSetsWithoutResources, Reference::get);
        } catch (SQLException e) {
            if (err != null) {
                e.addSuppressed(err);
            }
            err = e;
        }
        if (err != null) {
            throw err;
        }
    }

    private <T> void closedRegisteredResultSetsImpl(Queue<T> queue, Function<T, ADBResultSet> rsAccessor)
            throws SQLException {
        SQLException err = null;
        T item;
        while ((item = queue.poll()) != null) {
            ADBResultSet rs = rsAccessor.apply(item);
            if (rs != null) {
                try {
                    rs.closeImpl(false);
                } catch (SQLException e) {
                    if (err != null) {
                        e.addSuppressed(err);
                    }
                    err = e;
                }
            }
        }
        if (err != null) {
            throw err;
        }
    }

    private static boolean isEmptyReference(Reference<ADBResultSet> ref) {
        return ref.get() == null;
    }

    // Result control

    @Override
    public void setLargeMaxRows(long maxRows) throws SQLException {
        checkClosed();
        if (maxRows < 0) {
            throw getErrorReporter().errorParameterValueNotSupported("maxRows");
        }
        this.maxRows = maxRows;
    }

    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        setLargeMaxRows(maxRows);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return (int) getLargeMaxRows();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "setCursorName");
    }

    // Unsupported hints (ignored)

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        switch (direction) {
            case ResultSet.FETCH_FORWARD:
            case ResultSet.FETCH_REVERSE:
            case ResultSet.FETCH_UNKNOWN:
                // ignore this hint
                break;
            default:
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
        if (rows < 0) {
            throw getErrorReporter().errorParameterNotSupported("rows");
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int maxFieldSize) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Statement.class, "setMaxFieldSize");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
    }

    // Errors and warnings

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return warnings != null ? connection.protocol.createSQLWarning(warnings) : null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        warnings = null;
    }

    @Override
    protected ADBErrorReporter getErrorReporter() {
        return connection.getErrorReporter();
    }

    protected Logger getLogger() {
        return connection.getLogger();
    }

    // Ownership

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    ADBStatement getResultSetStatement(ADBResultSet rs) {
        return rs.metadata.statement;
    }

    // Serialization

    static void configureSerialization(SimpleModule serdeModule) {
        serdeModule.setSerializerModifier(createADMFormatSerializerModifier());
    }

    static AbstractValueSerializer getADMFormatSerializer(Class<?> cls) {
        return SERIALIZER_MAP.get(cls);
    }

    private static BeanSerializerModifier createADMFormatSerializerModifier() {
        return new BeanSerializerModifier() {
            @Override
            public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc,
                    JsonSerializer<?> serializer) {
                Class<?> cls = beanDesc.getClassInfo().getAnnotated();
                if (isSetObjectCompatible(cls)) {
                    AbstractValueSerializer ser = getADMFormatSerializer(cls);
                    return ser != null ? ser : super.modifySerializer(config, beanDesc, serializer);
                } else {
                    return null;
                }
            }
        };
    }

    static boolean isSetObjectCompatible(Class<?> cls) {
        if (ADBRowStore.OBJECT_ACCESSORS_ATOMIC.containsKey(cls)) {
            return true;
        }
        for (Class<?> aClass : SET_OBJECT_NON_ATOMIC) {
            if (aClass.isAssignableFrom(cls)) {
                return true;
            }
        }
        return false;
    }

    private static Map<Class<?>, AbstractValueSerializer> createSerializerMap() {
        Map<Class<?>, AbstractValueSerializer> serializerMap = new HashMap<>();
        registerSerializer(serializerMap, createGenericSerializer(Byte.class, ADBDatatype.TINYINT));
        registerSerializer(serializerMap, createGenericSerializer(Short.class, ADBDatatype.SMALLINT));
        registerSerializer(serializerMap, createGenericSerializer(Integer.class, ADBDatatype.INTEGER));
        registerSerializer(serializerMap, createGenericSerializer(UUID.class, ADBDatatype.UUID));
        // Long is serialized as JSON number by Jackson
        registerSerializer(serializerMap, createFloatSerializer());
        registerSerializer(serializerMap, createDoubleSerializer());
        registerSerializer(serializerMap, createStringSerializer());
        registerSerializer(serializerMap, createSqlDateSerializer());
        registerSerializer(serializerMap, createLocalDateSerializer());
        registerSerializer(serializerMap, createSqlTimeSerializer());
        registerSerializer(serializerMap, createLocalTimeSerializer());
        registerSerializer(serializerMap, createSqlTimestampSerializer());
        registerSerializer(serializerMap, createInstantSerializer());
        registerSerializer(serializerMap, createPeriodSerializer());
        registerSerializer(serializerMap, createDurationSerializer());
        return serializerMap;
    }

    private static void registerSerializer(Map<Class<?>, AbstractValueSerializer> map,
            AbstractValueSerializer serializer) {
        map.put(serializer.getJavaType(), serializer);
    }

    private static ATaggedValueSerializer createGenericSerializer(Class<?> javaType, ADBDatatype ADBDatatype) {
        return new ATaggedValueSerializer(javaType, ADBDatatype) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                out.append(value);
            }
        };
    }

    private static AbstractValueSerializer createStringSerializer() {
        return new AbstractValueSerializer(java.lang.String.class) {
            @Override
            public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeString(serializeToString(value));
            }

            @Override
            protected String serializeToString(Object value) {
                return ADBProtocol.TEXT_DELIMITER + String.valueOf(value);
            }
        };
    }

    private static ATaggedValueSerializer createFloatSerializer() {
        return new ATaggedValueSerializer(Float.class, ADBDatatype.FLOAT) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                int bits = Float.floatToIntBits((Float) value);
                out.append((long) bits);
            }
        };
    }

    private static ATaggedValueSerializer createDoubleSerializer() {
        return new ATaggedValueSerializer(Double.class, ADBDatatype.DOUBLE) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                long bits = Double.doubleToLongBits((Double) value);
                out.append(bits);
            }
        };
    }

    private static ATaggedValueSerializer createSqlDateSerializer() {
        return new ATaggedValueSerializer(java.sql.Date.class, ADBDatatype.DATE) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                long millis = ((Date) value).getTime();
                out.append(millis);
            }
        };
    }

    private static ATaggedValueSerializer createLocalDateSerializer() {
        return new ATaggedValueSerializer(java.time.LocalDate.class, ADBDatatype.DATE) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                long millis = TimeUnit.DAYS.toMillis(((LocalDate) value).toEpochDay());
                out.append(millis);
            }
        };
    }

    private static ATaggedValueSerializer createSqlTimeSerializer() {
        return new ATaggedValueSerializer(java.sql.Time.class, ADBDatatype.TIME) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                long millis = ((Time) value).getTime();
                out.append(millis);
            }
        };
    }

    private static ATaggedValueSerializer createLocalTimeSerializer() {
        return new ATaggedValueSerializer(java.time.LocalTime.class, ADBDatatype.TIME) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                long millis = TimeUnit.NANOSECONDS.toMillis(((LocalTime) value).toNanoOfDay());
                out.append(millis);
            }
        };
    }

    private static ATaggedValueSerializer createSqlTimestampSerializer() {
        return new ATaggedValueSerializer(java.sql.Timestamp.class, ADBDatatype.DATETIME) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                long millis = ((Timestamp) value).getTime();
                out.append(millis);
            }
        };
    }

    private static ATaggedValueSerializer createInstantSerializer() {
        return new ATaggedValueSerializer(java.time.Instant.class, ADBDatatype.DATETIME) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                long millis = ((Instant) value).toEpochMilli();
                out.append(millis);
            }
        };
    }

    private static ATaggedValueSerializer createPeriodSerializer() {
        return new ATaggedValueSerializer(java.time.Period.class, ADBDatatype.YEARMONTHDURATION) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                long months = ((Period) value).toTotalMonths();
                out.append(months);
            }
        };
    }

    private static ATaggedValueSerializer createDurationSerializer() {
        return new ATaggedValueSerializer(java.time.Duration.class, ADBDatatype.DAYTIMEDURATION) {
            @Override
            protected void serializeNonTaggedValue(Object value, StringBuilder out) {
                long millis = ((Duration) value).toMillis();
                out.append(millis);
            }
        };
    }

    static abstract class AbstractValueSerializer extends JsonSerializer<Object> {

        protected final Class<?> javaType;

        protected AbstractValueSerializer(Class<?> javaType) {
            this.javaType = Objects.requireNonNull(javaType);
        }

        protected Class<?> getJavaType() {
            return javaType;
        }

        abstract String serializeToString(Object value);
    }

    private static abstract class ATaggedValueSerializer extends AbstractValueSerializer {

        protected final ADBDatatype adbType;

        protected ATaggedValueSerializer(Class<?> javaType, ADBDatatype adbType) {
            super(javaType);
            this.adbType = Objects.requireNonNull(adbType);
        }

        @Override
        public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeString(serializeToString(value)); // TODO:optimize?
        }

        protected final String serializeToString(Object value) {
            StringBuilder textBuilder = new StringBuilder(64); // TODO:optimize?
            printByteAsHex(adbType.getTypeTag(), textBuilder);
            textBuilder.append(ADBProtocol.TEXT_DELIMITER);
            serializeNonTaggedValue(value, textBuilder);
            return textBuilder.toString();
        }

        protected abstract void serializeNonTaggedValue(Object value, StringBuilder out);

        private static void printByteAsHex(byte b, StringBuilder out) {
            out.append(hex((b >>> 4) & 0x0f));
            out.append(hex(b & 0x0f));
        }

        private static char hex(int i) {
            return (char) (i + (i < 10 ? '0' : ('A' - 10)));
        }
    }
}
