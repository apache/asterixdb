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

import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ADBMetaStatement extends ADBStatement {

    public static final String SCHEMALESS = "SCHEMALESS";
    public static final String TABLE = "TABLE";
    public static final String VIEW = "VIEW";

    protected ADBMetaStatement(ADBConnection connection) {
        super(connection, null, null);
    }

    ADBResultSet executeGetCatalogsQuery() throws SQLException {
        checkClosed();

        StringBuilder sql = new StringBuilder(256);

        sql.append("select TABLE_CAT ");
        sql.append("from Metadata.`Dataverse` ");
        sql.append("let name = decode_dataverse_name(DataverseName), ");
        sql.append("TABLE_CAT = name[0] ");
        sql.append("where array_length(name) between 1 and 2 ");
        sql.append("group by TABLE_CAT ");
        sql.append("order by TABLE_CAT");

        return executeQueryImpl(sql.toString(), null);
    }

    ADBResultSet executeGetSchemasQuery(String catalog, String schemaPattern) throws SQLException {
        checkClosed();

        StringBuilder sql = new StringBuilder(512);
        sql.append("select TABLE_SCHEM, TABLE_CATALOG ");
        sql.append("from Metadata.`Dataverse` ");
        sql.append("let name = decode_dataverse_name(DataverseName), ");
        sql.append("TABLE_CATALOG = name[0], ");
        sql.append("TABLE_SCHEM = case array_length(name) when 1 then null else name[1] end ");
        sql.append("where array_length(name) between 1 and 2 ");
        if (catalog != null) {
            sql.append("and TABLE_CATALOG = $1 ");
        }
        if (schemaPattern != null) {
            sql.append("and if_null(TABLE_SCHEM, '') like $2 ");
        }
        sql.append("order by TABLE_CATALOG, TABLE_SCHEM");

        return executeQueryImpl(sql.toString(), Arrays.asList(catalog, schemaPattern));
    }

    ADBResultSet executeGetTablesQuery(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        checkClosed();

        String datasetTermTabular = getDatasetTerm(true);
        String datasetTermNonTabular = getDatasetTerm(false);
        String viewTermTabular = getViewTerm(true);
        String viewTermNonTabular = getViewTerm(false);

        StringBuilder sql = new StringBuilder(1024);
        sql.append("select TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, null REMARKS, null TYPE_CAT, ");
        sql.append("null TYPE_SCHEM, null TYPE_NAME, null SELF_REFERENCING_COL_NAME, null REF_GENERATION ");
        sql.append("from Metadata.`Dataset` ds join Metadata.`Datatype` dt ");
        sql.append("on ds.DatatypeDataverseName = dt.DataverseName and ds.DatatypeName = dt.DatatypeName ");
        sql.append("let dvname = decode_dataverse_name(ds.DataverseName), ");
        sql.append("isDataset = (ds.DatasetType = 'INTERNAL' or ds.DatasetType = 'EXTERNAL'), ");
        sql.append("isView = ds.DatasetType = 'VIEW', ");
        sql.append("hasFields = array_length(dt.Derived.Record.Fields) > 0, ");
        sql.append("TABLE_CAT = dvname[0], ");
        sql.append("TABLE_SCHEM = case array_length(dvname) when 1 then null else dvname[1] end, ");
        sql.append("TABLE_NAME = ds.DatasetName, ");
        sql.append("TABLE_TYPE = case ");
        sql.append("when isDataset then (case when hasFields then '").append(datasetTermTabular).append("' else '")
                .append(datasetTermNonTabular).append("' end) ");
        sql.append("when isView then (case when hasFields then '").append(viewTermTabular).append("' else '")
                .append(viewTermNonTabular).append("' end) ");
        sql.append("else null end ");
        sql.append("where array_length(dvname) between 1 and 2 ");
        if (catalog != null) {
            sql.append("and TABLE_CAT = $1 ");
        }
        if (schemaPattern != null) {
            sql.append("and if_null(TABLE_SCHEM, '') like $2 ");
        }
        if (tableNamePattern != null) {
            sql.append("and TABLE_NAME like $3 ");
        }
        sql.append("and TABLE_TYPE ").append(types != null ? "in $4" : "is not null").append(" ");
        sql.append("order by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME");

        return executeQueryImpl(sql.toString(),
                Arrays.asList(catalog, schemaPattern, tableNamePattern, types != null ? Arrays.asList(types) : null));
    }

    ADBResultSet executeGetColumnsQuery(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        checkClosed();

        StringBuilder sql = new StringBuilder(2048);
        sql.append("select TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_SIZE, ");
        sql.append("1 BUFFER_LENGTH, null DECIMAL_DIGITS, 2 NUM_PREC_RADIX, NULLABLE, ");
        sql.append("null REMARKS, null COLUMN_DEF, DATA_TYPE SQL_DATA_TYPE,");
        sql.append("0 SQL_DATETIME_SUB, COLUMN_SIZE CHAR_OCTET_LENGTH, ORDINAL_POSITION, ");
        sql.append("case NULLABLE when 0 then 'NO' else 'YES' end IS_NULLABLE, ");
        sql.append("null SCOPE_CATALOG, null SCOPE_SCHEMA, null SCOPE_TABLE, null SOURCE_DATA_TYPE, ");
        sql.append("'NO' IS_AUTOINCREMENT, 'NO' IS_GENERATEDCOLUMN ");
        sql.append("from Metadata.`Dataset` ds ");
        sql.append("join Metadata.`Datatype` dt ");
        sql.append("on ds.DatatypeDataverseName = dt.DataverseName and ds.DatatypeName = dt.DatatypeName ");
        sql.append("unnest dt.Derived.Record.Fields as field at fieldpos ");
        sql.append("left join Metadata.`Datatype` dt2 ");
        sql.append(
                "on field.FieldType = dt2.DatatypeName and ds.DataverseName = dt2.DataverseName and dt2.Derived is known ");
        sql.append("let dvname = decode_dataverse_name(ds.DataverseName), ");
        sql.append("TABLE_CAT = dvname[0], ");
        sql.append("TABLE_SCHEM = case array_length(dvname) when 1 then null else dvname[1] end, ");
        sql.append("TABLE_NAME = ds.DatasetName, ");
        sql.append("COLUMN_NAME = field.FieldName, ");
        sql.append("TYPE_NAME = case ");
        for (ADBDatatype nestedType : new ADBDatatype[] { ADBDatatype.OBJECT, ADBDatatype.ARRAY,
                ADBDatatype.MULTISET }) {
            sql.append(String.format("when dt2.Derived.%s is known then '%s' ",
                    ADBDatatype.getDerivedRecordName(nestedType), nestedType.getTypeName()));
        }
        sql.append("else field.FieldType end, ");
        sql.append("DATA_TYPE = ");
        sql.append("case TYPE_NAME ");
        for (ADBDatatype type : ADBDatatype.values()) {
            JDBCType jdbcType = type.getJdbcType();
            if (type.isNullOrMissing() || jdbcType.equals(JDBCType.OTHER)) {
                // will be handled by the 'else' clause
                continue;
            }
            sql.append("when '").append(type.getTypeName()).append("' ");
            sql.append("then ").append(jdbcType.getVendorTypeNumber()).append(" ");
        }
        sql.append("else ").append(JDBCType.OTHER.getVendorTypeNumber()).append(" end, ");

        sql.append("COLUMN_SIZE = case field.FieldType when 'string' then 32767 else 8 end, "); // TODO:based on type
        sql.append("ORDINAL_POSITION = fieldpos, ");
        sql.append("NULLABLE = case when field.IsNullable or field.IsMissable then 1 else 0 end ");
        sql.append("where array_length(dvname) between 1 and 2 ");

        sql.append("and array_length(dt.Derived.Record.Fields) > 0 ");
        if (catalog != null) {
            sql.append("and TABLE_CAT = $1 ");
        }
        if (schemaPattern != null) {
            sql.append("and if_null(TABLE_SCHEM, '') like $2 ");
        }
        if (tableNamePattern != null) {
            sql.append("and TABLE_NAME like $3 ");
        }
        if (columnNamePattern != null) {
            sql.append("and COLUMN_NAME like $4 ");
        }
        sql.append("order by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION");

        return executeQueryImpl(sql.toString(),
                Arrays.asList(catalog, schemaPattern, tableNamePattern, columnNamePattern));
    }

    ADBResultSet executeGetPrimaryKeysQuery(String catalog, String schema, String table) throws SQLException {
        checkClosed();

        StringBuilder sql = new StringBuilder(1024);
        sql.append("select TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, KEY_SEQ, null PK_NAME ");
        sql.append("from Metadata.`Dataset` ds unnest ds.InternalDetails.PrimaryKey pki at pkipos ");
        sql.append("let dvname = decode_dataverse_name(ds.DataverseName), ");
        sql.append("TABLE_CAT = dvname[0], ");
        sql.append("TABLE_SCHEM = case array_length(dvname) when 1 then null else dvname[1] end, ");
        sql.append("TABLE_NAME = ds.DatasetName, ");
        sql.append("COLUMN_NAME = pki[0], ");
        sql.append("KEY_SEQ = pkipos ");
        sql.append("where array_length(dvname) between 1 and 2 ");
        sql.append("and (every pk in ds.InternalDetails.PrimaryKey satisfies array_length(pk) = 1 end) ");
        sql.append("and (every si in ds.InternalDetails.KeySourceIndicator satisfies si = 0 end ) ");
        if (catalog != null) {
            sql.append("and TABLE_CAT = $1 ");
        }
        if (schema != null) {
            sql.append("and if_null(TABLE_SCHEM, '') like $2 ");
        }
        if (table != null) {
            sql.append("and TABLE_NAME like $3 ");
        }
        sql.append("order by COLUMN_NAME");

        return executeQueryImpl(sql.toString(), Arrays.asList(catalog, schema, table));
    }

    ADBResultSet executeGetTableTypesQuery() throws SQLException {
        checkClosed();

        LinkedHashSet<String> tableTypes = new LinkedHashSet<>();
        tableTypes.add(getDatasetTerm(true));
        tableTypes.add(getDatasetTerm(false));
        tableTypes.add(getViewTerm(true));
        tableTypes.add(getViewTerm(false));

        List<ADBColumn> columns = Collections.singletonList(new ADBColumn("TABLE_TYPE", ADBDatatype.STRING, false));

        AbstractValueSerializer stringSer = getADMFormatSerializer(String.class);
        ArrayNode result = (ArrayNode) connection.protocol.driverContext.genericObjectReader.createArrayNode();
        for (String tableType : tableTypes) {
            result.addObject().put("TABLE_TYPE", stringSer.serializeToString(tableType));
        }

        return createSystemResultSet(columns, result);
    }

    ADBResultSet executeGetTypeInfoQuery() throws SQLException {
        checkClosed();

        AbstractValueSerializer int16Ser = getADMFormatSerializer(Short.class);
        AbstractValueSerializer int32Ser = getADMFormatSerializer(Integer.class);
        AbstractValueSerializer stringSer = getADMFormatSerializer(String.class);

        List<ADBColumn> columns = new ArrayList<>();
        columns.add(new ADBColumn("TYPE_NAME", ADBDatatype.STRING, false));
        columns.add(new ADBColumn("DATA_TYPE", ADBDatatype.INTEGER, false));
        columns.add(new ADBColumn("PRECISION", ADBDatatype.INTEGER, true));
        columns.add(new ADBColumn("LITERAL_PREFIX", ADBDatatype.STRING, true));
        columns.add(new ADBColumn("LITERAL_SUFFIX", ADBDatatype.STRING, true));
        columns.add(new ADBColumn("CREATE_PARAMS", ADBDatatype.STRING, true));
        columns.add(new ADBColumn("NULLABLE", ADBDatatype.SMALLINT, true));
        columns.add(new ADBColumn("CASE_SENSITIVE", ADBDatatype.BOOLEAN, true));
        columns.add(new ADBColumn("SEARCHABLE", ADBDatatype.SMALLINT, true));
        columns.add(new ADBColumn("UNSIGNED_ATTRIBUTE", ADBDatatype.BOOLEAN, true));
        columns.add(new ADBColumn("FIXED_PREC_SCALE", ADBDatatype.BOOLEAN, true));
        columns.add(new ADBColumn("AUTO_INCREMENT", ADBDatatype.BOOLEAN, true));
        columns.add(new ADBColumn("LOCAL_TYPE_NAME", ADBDatatype.STRING, true));
        columns.add(new ADBColumn("MINIMUM_SCALE", ADBDatatype.SMALLINT, true));
        columns.add(new ADBColumn("MAXIMUM_SCALE", ADBDatatype.SMALLINT, true));
        columns.add(new ADBColumn("SQL_DATA_TYPE", ADBDatatype.INTEGER, true));
        columns.add(new ADBColumn("SQL_DATETIME_SUB", ADBDatatype.INTEGER, true));
        columns.add(new ADBColumn("NUM_PREC_RADIX", ADBDatatype.INTEGER, true));

        ArrayNode result = (ArrayNode) connection.protocol.driverContext.genericObjectReader.createArrayNode();
        populateTypeInfo(result.addObject(), ADBDatatype.BOOLEAN, 1, null, null, null, null, null, null, int16Ser,
                int32Ser, stringSer);
        populateTypeInfo(result.addObject(), ADBDatatype.TINYINT, 3, 10, 0, 0, false, null, null, int16Ser, int32Ser,
                stringSer);
        populateTypeInfo(result.addObject(), ADBDatatype.SMALLINT, 5, 10, 0, 0, false, null, null, int16Ser, int32Ser,
                stringSer);
        populateTypeInfo(result.addObject(), ADBDatatype.INTEGER, 10, 10, 0, 0, false, null, null, int16Ser, int32Ser,
                stringSer);
        populateTypeInfo(result.addObject(), ADBDatatype.BIGINT, 19, 10, 0, 0, false, null, null, int16Ser, int32Ser,
                stringSer);
        populateTypeInfo(result.addObject(), ADBDatatype.FLOAT, 7, 2, 0, 0, false, null, null, int16Ser, int32Ser,
                stringSer);
        populateTypeInfo(result.addObject(), ADBDatatype.DOUBLE, 15, 2, 0, 0, false, null, null, int16Ser, int32Ser,
                stringSer);
        populateTypeInfo(result.addObject(), ADBDatatype.DATE, 32, null, 0, 0, false, null, null, int16Ser, int32Ser,
                stringSer); // TODO:precision
        populateTypeInfo(result.addObject(), ADBDatatype.TIME, 32, null, 0, 0, false, null, null, int16Ser, int32Ser,
                stringSer); // TODO:precision
        populateTypeInfo(result.addObject(), ADBDatatype.DATETIME, 32, null, 0, 0, false, null, null, int16Ser,
                int32Ser, stringSer); // TODO:precision
        populateTypeInfo(result.addObject(), ADBDatatype.YEARMONTHDURATION, 32, null, 0, 0, false, null, null, int16Ser,
                int32Ser, stringSer); // TODO:precision
        populateTypeInfo(result.addObject(), ADBDatatype.DAYTIMEDURATION, 32, null, 0, 0, false, null, null, int16Ser,
                int32Ser, stringSer); // TODO:precision
        populateTypeInfo(result.addObject(), ADBDatatype.DURATION, 32, null, 0, 0, false, null, null, int16Ser,
                int32Ser, stringSer); // TODO:precision
        populateTypeInfo(result.addObject(), ADBDatatype.STRING, 32767, null, null, null, true, "'", "'", int16Ser,
                int32Ser, stringSer);
        populateTypeInfo(result.addObject(), ADBDatatype.ARRAY, 32767, null, 0, 0, false, null, null, int16Ser,
                int32Ser, stringSer);
        populateTypeInfo(result.addObject(), ADBDatatype.OBJECT, 32767, null, 0, 0, false, null, null, int16Ser,
                int32Ser, stringSer);

        return createSystemResultSet(columns, result);
    }

    private void populateTypeInfo(ObjectNode typeInfo, ADBDatatype type, int precision, Integer precisionRadix,
            Integer minScale, Integer maxScale, Boolean searchable, String literalPrefix, String literalSuffix,
            ADBPreparedStatement.AbstractValueSerializer int16Ser,
            ADBPreparedStatement.AbstractValueSerializer int32Ser,
            ADBPreparedStatement.AbstractValueSerializer stringSer) {
        typeInfo.put("TYPE_NAME", stringSer.serializeToString(type.getTypeName()));
        typeInfo.put("DATA_TYPE", int32Ser.serializeToString(type.getJdbcType().getVendorTypeNumber()));
        typeInfo.put("PRECISION", int32Ser.serializeToString(precision));
        typeInfo.put("LITERAL_PREFIX", literalPrefix != null ? stringSer.serializeToString(literalPrefix) : null);
        typeInfo.put("LITERAL_SUFFIX", literalSuffix != null ? stringSer.serializeToString(literalSuffix) : null);
        typeInfo.putNull("CREATE_PARAMS");
        typeInfo.put("NULLABLE", int16Ser.serializeToString((short) DatabaseMetaData.typeNullable));
        typeInfo.put("CASE_SENSITIVE", false);
        typeInfo.put("SEARCHABLE",
                int16Ser.serializeToString((short) (searchable == null ? DatabaseMetaData.typePredNone
                        : searchable ? DatabaseMetaData.typeSearchable : DatabaseMetaData.typePredBasic)));
        typeInfo.put("UNSIGNED_ATTRIBUTE", false);
        typeInfo.put("FIXED_PREC_SCALE", false);
        typeInfo.putNull("AUTO_INCREMENT");
        typeInfo.putNull("LOCAL_TYPE_NAME");
        typeInfo.put("MINIMUM_SCALE", minScale != null ? int16Ser.serializeToString(minScale.shortValue()) : null);
        typeInfo.put("MAXIMUM_SCALE", maxScale != null ? int16Ser.serializeToString(maxScale.shortValue()) : null);
        typeInfo.put("SQL_DATA_TYPE", int32Ser.serializeToString(type.getTypeTag()));
        typeInfo.putNull("SQL_DATETIME_SUB");
        typeInfo.put("NUM_PREC_RADIX", int32Ser.serializeToString(precisionRadix != null ? precisionRadix : 10));
    }

    ADBResultSet executeEmptyResultQuery() throws SQLException {
        checkClosed();
        return createEmptyResultSet();
    }

    @Override
    ADBStatement getResultSetStatement(ADBResultSet rs) {
        return null;
    }

    protected String getDatasetTerm(boolean tabular) {
        return tabular ? TABLE : SCHEMALESS + " " + TABLE;
    }

    protected String getViewTerm(boolean tabular) {
        return tabular ? VIEW : SCHEMALESS + " " + VIEW;
    }
}
