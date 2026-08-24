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
package org.apache.asterix.common.metadata;

import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;

public class MetadataUtil {
    public static final int PENDING_NO_OP = 0;
    public static final int PENDING_ADD_OP = 1;
    public static final int PENDING_DROP_OP = 2;

    private MetadataUtil() {
    }

    public static String pendingOpToString(int pendingOp) {
        switch (pendingOp) {
            case PENDING_NO_OP:
                return "Pending No Operation";
            case PENDING_ADD_OP:
                return "Pending Add Operation";
            case PENDING_DROP_OP:
                return "Pending Drop Operation";
            default:
                return "Unknown Pending Operation";
        }
    }

    public static String dataverseName(String databaseName, DataverseName dataverseName, boolean useDb) {
        return useDb ? databaseName + "." + dataverseName : String.valueOf(dataverseName);
    }

    public static String getFullyQualifiedDisplayName(DataverseName dataverseName, String objectName) {
        return dataverseName + "." + objectName;
    }

    public static String getFullyQualifiedDisplayName(String databaseName, DataverseName dataverseName,
            String objectName) {
        return databaseName + "." + dataverseName + "." + objectName;
    }

    /**
     * Returns the fully qualified name with every part quoted, so that it can be embedded in a statement.
     *
     * @param databaseName
     * @param dataverseName
     * @param objectName
     */
    public static String getQuotedFullyQualifiedDisplayName(String databaseName, DataverseName dataverseName,
            String objectName) {
        StringBuilder out = new StringBuilder();
        appendQuoted(out, databaseName);
        for (String dataversePart : dataverseName.getParts()) {
            out.append(DataverseName.DISPLAY_FORM_SEPARATOR_CHAR);
            appendQuoted(out, dataversePart);
        }
        out.append(DataverseName.DISPLAY_FORM_SEPARATOR_CHAR);
        appendQuoted(out, objectName);
        return out.toString();
    }

    private static void appendQuoted(StringBuilder out, String part) {
        out.append(DataverseName.DISPLAY_FORM_QUOTE_CHAR);
        for (int i = 0; i < part.length(); i++) {
            char c = part.charAt(i);
            if (c == DataverseName.DISPLAY_FORM_ESCAPE_CHAR || c == DataverseName.DISPLAY_FORM_QUOTE_CHAR) {
                out.append(DataverseName.DISPLAY_FORM_ESCAPE_CHAR);
            }
            out.append(c);
        }
        out.append(DataverseName.DISPLAY_FORM_QUOTE_CHAR);
    }

    public static String databaseFor(DataverseName dataverse) {
        if (dataverse == null) {
            return null;
        }
        if (MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverse)) {
            return MetadataConstants.SYSTEM_DATABASE;
        } else if (FunctionConstants.ASTERIX_DV.equals(dataverse)) {
            return FunctionConstants.ASTERIX_DB;
        } else if (FunctionConstants.ALGEBRICKS_DV.equals(dataverse)) {
            return AlgebricksBuiltinFunctions.ALGEBRICKS_DB;
        } else {
            return MetadataConstants.DEFAULT_DATABASE;
        }
    }

    public static String resolveDatabase(String database, DataverseName dataverse) {
        if (dataverse == null) {
            return null;
        }
        if (database != null) {
            return database;
        }
        return databaseFor(dataverse);
    }
}
