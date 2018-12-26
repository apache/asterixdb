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
package org.apache.asterix.lang.sqlpp.util;

public class SqlppStatementUtil {

    private static final String IF_EXISTS = " IF EXISTS ";
    private static final String IF_NOT_EXISTS = " IF NOT EXISTS ";
    private static final String CREATE_DATAVERSE = "CREATE DATAVERSE ";
    public static final String CREATE_DATASET = "CREATE DATASET ";
    private static final String DROP_DATASET = "DROP DATASET ";
    private static final String CREATE_INDEX = "CREATE INDEX ";
    private static final String CREATE_PRIMARY_INDEX = "CREATE PRIMARY INDEX ";
    private static final String DROP_INDEX = "DROP INDEX ";
    public static final String ON = " ON ";
    public static final String WHERE = " WHERE ";
    public static final char SEMI_COLON = ';';
    public static final char DOT = '.';
    public static final char COLON = ':';
    public static final char COMMA = ',';
    public static final char L_PARENTHESIS = '(';
    public static final char R_PARENTHESIS = ')';
    public static final char L_BRACE = '{';
    public static final char R_BRACE = '}';
    private static final char QUOTE = '\"';
    private static final char BACK_TICK = '`';

    private SqlppStatementUtil() {
    }

    @SuppressWarnings("squid:S1172") // unused variable
    public static StringBuilder getCreateDataverseStatement(StringBuilder stringBuilder, String dataverseName,
            boolean ifNotExists, int version) {
        stringBuilder.append(CREATE_DATAVERSE);
        enclose(stringBuilder, dataverseName);
        return ifNotExists(stringBuilder, ifNotExists).append(SEMI_COLON);
    }

    @SuppressWarnings("squid:S1172") // unused variable
    public static StringBuilder getDropDatasetStatement(StringBuilder stringBuilder, String dataverseName,
            String datasetName, boolean ifExists, int version) {
        stringBuilder.append(DROP_DATASET);
        enclose(stringBuilder, dataverseName, datasetName);
        return ifExists(stringBuilder, ifExists).append(SEMI_COLON);
    }

    @SuppressWarnings("squid:S1172") // unused variable
    public static StringBuilder getCreateIndexStatement(StringBuilder stringBuilder, String dataverseName,
            String datasetName, String indexName, String fields, int version) {
        stringBuilder.append(CREATE_INDEX);
        enclose(stringBuilder, indexName).append(ON);
        return enclose(stringBuilder, dataverseName, datasetName).append(fields).append(SEMI_COLON);
    }

    @SuppressWarnings("squid:S1172") // unused variable
    public static StringBuilder getCreatePrimaryIndexStatement(StringBuilder stringBuilder, String dataverseName,
            String datasetName, String indexName, int version) {
        stringBuilder.append(CREATE_PRIMARY_INDEX);
        enclose(stringBuilder, indexName).append(ON);
        return enclose(stringBuilder, dataverseName, datasetName).append(SEMI_COLON);
    }

    @SuppressWarnings("squid:S1172") // unused variable
    public static StringBuilder getDropIndexStatement(StringBuilder stringBuilder, String dataverseName,
            String datasetName, String indexName, boolean ifExists, int version) {
        stringBuilder.append(DROP_INDEX);
        enclose(stringBuilder, dataverseName, datasetName, indexName);
        return ifExists(stringBuilder, ifExists).append(SEMI_COLON);
    }

    private static StringBuilder ifExists(StringBuilder stringBuilder, boolean ifExists) {
        return ifExists ? stringBuilder.append(IF_EXISTS) : stringBuilder;
    }

    private static StringBuilder ifNotExists(StringBuilder stringBuilder, boolean ifNotExists) {
        return ifNotExists ? stringBuilder.append(IF_NOT_EXISTS) : stringBuilder;
    }

    /**
     * Encloses the {@param identifier} in back-ticks.
     * @param stringBuilder where the identifier will be appended
     * @param identifier an identifier which could be a valid identifier or one that needs to be delimited
     * @return {@param stringBuilder} with the <i>delimited</i> identifier appended
     */
    public static StringBuilder enclose(StringBuilder stringBuilder, String identifier) {
        return stringBuilder.append(BACK_TICK).append(identifier).append(BACK_TICK);
    }

    /**
     * Same as {@link SqlppStatementUtil#enclose(StringBuilder, String)} but for a qualified identifier.
     * @param stringBuilder where the identifier will be appended
     * @param identifier1 the qualifying identifier
     * @param identifier2 the qualified identifier
     * @return {@param stringBuilder} with the <i>delimited</i> qualified identifier appended
     */
    public static StringBuilder enclose(StringBuilder stringBuilder, String identifier1, String identifier2) {
        return stringBuilder.append(BACK_TICK).append(identifier1).append(BACK_TICK).append(DOT).append(BACK_TICK)
                .append(identifier2).append(BACK_TICK);
    }

    /**
     * Same as {@link SqlppStatementUtil#enclose(StringBuilder, String)} but for a double qualified identifier.
     * @param stringBuilder where the identifier will be appended
     * @param identifier1 the 1st qualifying identifier
     * @param identifier2 the 2nd qualifying identifier
     * @param identifier3 the qualified identifier
     * @return {@param stringBuilder} with the <i>delimited</i> qualified identifier appended
     */
    public static StringBuilder enclose(StringBuilder stringBuilder, String identifier1, String identifier2,
            String identifier3) {
        return stringBuilder.append(BACK_TICK).append(identifier1).append(BACK_TICK).append(DOT).append(BACK_TICK)
                .append(identifier2).append(BACK_TICK).append(DOT).append(BACK_TICK).append(identifier3)
                .append(BACK_TICK);
    }

    public static String enclose(String identifier) {
        return BACK_TICK + identifier + BACK_TICK;
    }

    public static String enclose(String identifier1, String identifier2) {
        return BACK_TICK + identifier1 + BACK_TICK + DOT + BACK_TICK + identifier2 + BACK_TICK;
    }

    public static StringBuilder quote(StringBuilder stringBuilder, String text) {
        return stringBuilder.append(QUOTE).append(text).append(QUOTE);
    }

    public static String quote(String text) {
        return QUOTE + text + QUOTE;
    }
}
