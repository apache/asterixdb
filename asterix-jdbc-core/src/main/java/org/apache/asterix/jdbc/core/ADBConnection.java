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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLPermission;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ADBConnection extends ADBWrapperSupport implements Connection {

    final ADBProtocol protocol;

    final String url;

    final String databaseVersion;

    private final AtomicBoolean closed;

    private final ConcurrentLinkedQueue<ADBStatement> statements;

    private volatile SQLWarning warning;

    private volatile ADBMetaStatement metaStatement;

    volatile String catalog;

    volatile String schema;

    // Lifecycle

    protected ADBConnection(ADBProtocol protocol, String url, String databaseVersion, String catalog, String schema,
            SQLWarning connectWarning) {
        this.url = Objects.requireNonNull(url);
        this.protocol = Objects.requireNonNull(protocol);
        this.databaseVersion = databaseVersion;
        this.statements = new ConcurrentLinkedQueue<>();
        this.warning = connectWarning;
        this.catalog = catalog;
        this.schema = schema;
        this.closed = new AtomicBoolean(false);
    }

    @Override
    public void close() throws SQLException {
        closeImpl(null);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw getErrorReporter().errorParameterValueNotSupported("executor");
        }
        SecurityManager sec = System.getSecurityManager();
        if (sec != null) {
            sec.checkPermission(new SQLPermission("callAbort"));
        }
        closeImpl(executor);
    }

    void closeImpl(Executor executor) throws SQLException {
        boolean wasClosed = closed.getAndSet(true);
        if (wasClosed) {
            return;
        }
        if (executor == null) {
            closeStatementsAndProtocol();
        } else {
            executor.execute(() -> {
                try {
                    closeStatementsAndProtocol();
                } catch (SQLException e) {
                    if (getLogger().isLoggable(Level.FINE)) {
                        getLogger().log(Level.FINE, e.getMessage(), e);
                    }
                }
            });
        }
    }

    private void closeStatementsAndProtocol() throws SQLException {
        SQLException err = null;
        try {
            closeRegisteredStatements();
        } catch (SQLException e) {
            err = e;
        }
        try {
            protocol.close();
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

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw getErrorReporter().errorObjectClosed(Connection.class);
        }
    }

    // Connectivity

    @Override
    public boolean isValid(int timeoutSeconds) throws SQLException {
        if (isClosed()) {
            return false;
        }
        if (timeoutSeconds < 0) {
            throw getErrorReporter().errorParameterValueNotSupported("timeoutSeconds");
        }
        return protocol.ping(timeoutSeconds);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "setNetworkTimeout");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "getNetworkTimeout");
    }

    // Metadata

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        ADBMetaStatement metaStatement = getOrCreateMetaStatement();
        return createDatabaseMetaData(metaStatement);
    }

    private ADBMetaStatement getOrCreateMetaStatement() {
        ADBMetaStatement stmt = metaStatement;
        if (stmt == null) {
            synchronized (this) {
                stmt = metaStatement;
                if (stmt == null) {
                    stmt = createMetaStatement();
                    registerStatement(stmt);
                    metaStatement = stmt;
                }
            }
        }
        return stmt;
    }

    protected ADBMetaStatement createMetaStatement() {
        return new ADBMetaStatement(this);
    }

    protected ADBDatabaseMetaData createDatabaseMetaData(ADBMetaStatement metaStatement) {
        return new ADBDatabaseMetaData(metaStatement, databaseVersion);
    }

    // Statement construction

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return createStatementImpl();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        checkClosed();
        checkResultSetConfig(resultSetType, resultSetConcurrency, resultSetHoldability);
        return createStatementImpl();
    }

    private void checkResultSetConfig(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        boolean ok = resultSetType == ResultSet.TYPE_FORWARD_ONLY && resultSetConcurrency == ResultSet.CONCUR_READ_ONLY;
        if (!ok) {
            throw getErrorReporter().errorParameterValueNotSupported("resultSetType/resultSetConcurrency");
        }
        if (resultSetHoldability != ADBResultSet.RESULT_SET_HOLDABILITY) {
            if (getLogger().isLoggable(Level.FINE)) {
                getLogger().log(Level.FINE,
                        getErrorReporter().warningParameterValueNotSupported("ResultSetHoldability"));
            }
        }
    }

    private ADBStatement createStatementImpl() {
        ADBStatement stmt = new ADBStatement(this, catalog, schema);
        registerStatement(stmt);
        return stmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return prepareStatementImpl(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        checkClosed();
        checkResultSetConfig(resultSetType, resultSetConcurrency, resultSetHoldability);
        return prepareStatementImpl(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareStatement");
    }

    private ADBPreparedStatement prepareStatementImpl(String sql) throws SQLException {
        ADBPreparedStatement stmt = new ADBPreparedStatement(this, sql, catalog, schema);
        registerStatement(stmt);
        return stmt;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareCall");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareCall");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "prepareCall");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return sql;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return catalog;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        this.catalog = catalog;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return schema;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        this.schema = schema;
    }

    // Statement lifecycle

    private void registerStatement(ADBStatement stmt) {
        statements.add(Objects.requireNonNull(stmt));
    }

    void deregisterStatement(ADBStatement stmt) {
        statements.remove(Objects.requireNonNull(stmt));
    }

    private void closeRegisteredStatements() throws SQLException {
        SQLException err = null;
        ADBStatement statement;
        while ((statement = statements.poll()) != null) {
            try {
                statement.closeImpl(true, false);
            } catch (SQLException e) {
                if (err != null) {
                    e.addSuppressed(err);
                }
                err = e;
            }
        }
        if (err != null) {
            throw err;
        }
    }

    // Transaction control

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        switch (level) {
            case Connection.TRANSACTION_READ_COMMITTED:
                break;
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                if (getLogger().isLoggable(Level.FINE)) {
                    getLogger().log(Level.FINE,
                            getErrorReporter().warningParameterValueNotSupported("TransactionIsolationLevel"));
                }
                break;
            default:
                throw getErrorReporter().errorParameterValueNotSupported("TransactionIsolationLevel");
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        switch (holdability) {
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                break;
            case ResultSet.CLOSE_CURSORS_AT_COMMIT:
                if (getLogger().isLoggable(Level.FINE)) {
                    getLogger().log(Level.FINE, getErrorReporter().warningParameterValueNotSupported("Holdability"));
                }
                break;
            default:
                throw getErrorReporter().errorParameterValueNotSupported("Holdability");
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("AutoCommit");
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        throw getErrorReporter().errorIncompatibleMode("AutoCommit");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "setSavepoint");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "releaseSavepoint");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "rollback");
    }

    // Value construction

    @Override
    public Clob createClob() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createClob");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createBlob");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createNClob");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createArrayOf");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createStruct");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "createSQLXML");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return Collections.emptyMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw getErrorReporter().errorMethodNotSupported(Connection.class, "setTypeMap");
    }

    // Unsupported hints (ignored)

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
    }

    // Errors and warnings

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return warning;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        warning = null;
    }

    @Override
    protected ADBErrorReporter getErrorReporter() {
        return protocol.driverContext.errorReporter;
    }

    protected Logger getLogger() {
        return protocol.driverContext.logger;
    }

    // Miscellaneous unsupported features (error is raised)

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return new Properties();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw getErrorReporter().errorClientInfoMethodNotSupported(Connection.class, "setClientInfo");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw getErrorReporter().errorClientInfoMethodNotSupported(Connection.class, "setClientInfo");
    }
}
