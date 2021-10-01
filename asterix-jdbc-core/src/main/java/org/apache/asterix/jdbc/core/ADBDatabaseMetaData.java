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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Objects;

final class ADBDatabaseMetaData extends ADBWrapperSupport implements DatabaseMetaData {

    /*
     * See org.apache.asterix.metadata.utils.MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8
     */
    private static final int METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 = 251;

    private final ADBMetaStatement metaStatement;

    private final String databaseVersionText;

    private volatile ADBProductVersion databaseVersion;

    ADBDatabaseMetaData(ADBMetaStatement metaStatement, String databaseVersionText) {
        this.metaStatement = Objects.requireNonNull(metaStatement);
        this.databaseVersionText = databaseVersionText;
    }

    // Driver name and version

    @Override
    public String getDriverName() {
        return metaStatement.connection.protocol.driverContext.driverVersion.productName;
    }

    @Override
    public String getDriverVersion() {
        return metaStatement.connection.protocol.driverContext.driverVersion.productVersion;
    }

    @Override
    public int getDriverMajorVersion() {
        return metaStatement.connection.protocol.driverContext.driverVersion.majorVersion;
    }

    @Override
    public int getDriverMinorVersion() {
        return metaStatement.connection.protocol.driverContext.driverVersion.minorVersion;
    }

    @Override
    public int getJDBCMajorVersion() {
        return ADBDriverBase.JDBC_MAJOR_VERSION;
    }

    @Override
    public int getJDBCMinorVersion() {
        return ADBDriverBase.JDBC_MINOR_VERSION;
    }

    // Database name and version

    @Override
    public String getDatabaseProductName() {
        return getDatabaseVersion().productName;
    }

    @Override
    public String getDatabaseProductVersion() {
        return getDatabaseVersion().productVersion;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return getDatabaseVersion().majorVersion;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return getDatabaseVersion().minorVersion;
    }

    private ADBProductVersion getDatabaseVersion() {
        ADBProductVersion result = databaseVersion;
        if (result == null) {
            databaseVersion = result = ADBProductVersion.parseDatabaseVersion(databaseVersionText);
        }
        return result;
    }

    // Database objects

    // Catalogs and schemas

    @Override
    public ADBResultSet getCatalogs() throws SQLException {
        return metaStatement.executeGetCatalogsQuery();
    }

    @Override
    public int getMaxCatalogNameLength() {
        return METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8;
    }

    @Override
    public ADBResultSet getSchemas() throws SQLException {
        return getSchemas(metaStatement.connection.catalog, null);
    }

    @Override
    public ADBResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return metaStatement.executeGetSchemasQuery(catalog, schemaPattern);
    }

    @Override
    public int getMaxSchemaNameLength() {
        return METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8;
    }

    // Tables

    @Override
    public ADBResultSet getTableTypes() throws SQLException {
        return metaStatement.executeGetTableTypesQuery();
    }

    @Override
    public ADBResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        return metaStatement.executeGetTablesQuery(catalog, schemaPattern, tableNamePattern, types);
    }

    @Override
    public ADBResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public int getMaxTableNameLength() {
        return METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    // Columns

    @Override
    public ADBResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        return metaStatement.executeGetColumnsQuery(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override
    public ADBResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public ADBResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public ADBResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        // TODO:primary keys?
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    // Keys

    @Override
    public ADBResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return metaStatement.executeGetPrimaryKeysQuery(catalog, schema, table);
    }

    @Override
    public ADBResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public ADBResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public ADBResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    // Indexes

    @Override
    public ADBResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    // Datatypes

    @Override
    public ADBResultSet getTypeInfo() throws SQLException {
        return metaStatement.executeGetTypeInfoQuery();
    }

    @Override
    public ADBResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public ADBResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public ADBResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public long getMaxLogicalLobSize() {
        return 0;
    }

    @Override
    public boolean supportsRefCursors() {
        return false;
    }

    // User-defined functions and procedures

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public ADBResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public ADBResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public ADBResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public ADBResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public int getMaxProcedureNameLength() {
        return METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8;
    }

    // Security

    @Override
    public ADBResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    @Override
    public ADBResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    // Other database objects

    @Override
    public ADBResultSet getClientInfoProperties() throws SQLException {
        return metaStatement.executeEmptyResultQuery();
    }

    // SQL dialect: general

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public String getSQLKeywords() {
        // keywords that are not also SQL:2003 keywords
        return "adapter,apply,asc,autogenerated,btree,closed,compaction,compact,correlate,collection,dataset,"
                + "dataverse,definition,desc,disconnect,div,explain,enforced,every,feed,flatten,fulltext,hints,if,"
                + "index,ingestion,internal,keyword,key,known,letting,let,limit,load,missing,mod,nodegroup,ngram,"
                + "offset,path,policy,pre-sorted,raw,refresh,returning,rtree,run,satisfies,secondary,some,stop,"
                + "synonym,temporary,type,upsert,use,view,write";
    }

    @Override
    public String getCatalogTerm() {
        return "catalog";
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getProcedureTerm() {
        return "procedure";
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    // SQL dialect: identifiers

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public String getIdentifierQuoteString() {
        return "`";
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    // SQL dialect: literals and parameters

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public boolean supportsNamedParameters() {
        // Procedures (CallableStatement) are not supported
        return false;
    }

    // SQL dialect: functions and operators

    @Override
    public String getNumericFunctions() {
        // NOTE: JDBC escape clause is not yet supported
        // "add,div,mod,mult,neg,sub,abs,acos,asin,atan,atan2,ceil,cos,deg,degrees,e,exp,ln,log,floor,inf,nan,neginf,pi,posinf,power,rad,radians,random,round,sign,sin,sqrt,tan,trunc";
        return "";
    }

    @Override
    public String getStringFunctions() {
        // NOTE: JDBC escape clause is not yet supported
        // "contains,initcap,length,lower,ltrim,position,pos,regex_contains,regex_like,regex_position,regex_pos,regex_replace,repeat,replace,rtrim,split,substr,title,trim,upper";
        return "";
    }

    @Override
    public String getSystemFunctions() {
        // NOTE: JDBC escape clause is not yet supported
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        // TODO:review
        return "current_date,current_time,current_datetime";
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    // SQL dialect: SELECT clause

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    // SQL dialect: FROM clause

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return true;
    }

    // SQL dialect: JOIN clause

    @Override
    public boolean supportsOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    // SQL dialect: ORDER BY clause

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    // SQL dialect: GROUP BY clause

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    // SQL dialect: Subquery

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    // SQL dialect: Set operations

    @Override
    public boolean supportsUnion() {
        return false;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    // SQL dialect: DML statements

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    // SQL dialect: DDL statements

    // DDL: CREATE DATASET

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    // DDL: CREATE INDEX

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return true;
    }

    // DDL: GRANT / REVOKE (not supported)

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    // SQL dialect: User-defined functions and procedures

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    // Transactions

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return true;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return Connection.TRANSACTION_READ_COMMITTED == level;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    // Connection

    @Override
    public Connection getConnection() throws SQLException {
        return metaStatement.connection;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public String getURL() {
        return metaStatement.connection.url;
    }

    @Override
    public String getUserName() {
        return metaStatement.connection.protocol.user;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    // Statement

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    // ResultSet

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ADBResultSet.RESULT_SET_HOLDABILITY;
    }

    @Override
    public int getResultSetHoldability() {
        return ADBResultSet.RESULT_SET_HOLDABILITY;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    // Miscellaneous

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    // Errors and warnings

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    protected ADBErrorReporter getErrorReporter() {
        return metaStatement.getErrorReporter();
    }
}
