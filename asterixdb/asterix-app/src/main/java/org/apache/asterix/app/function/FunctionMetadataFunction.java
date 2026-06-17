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
package org.apache.asterix.app.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.lang.common.util.CommonFunctionMapUtil;
import org.apache.asterix.metadata.declared.AbstractDatasourceFunction;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FunctionMetadataFunction extends AbstractDatasourceFunction {

    private static final long serialVersionUID = 2L;

    static final String KIND_BUILTIN = "builtin";
    static final String KIND_UDF = "udf";

    // UDF rows are resolved on the CC (they live in the metadata catalog) and shipped here.
    private final List<String> udfRecords;

    public FunctionMetadataFunction(AlgebricksAbsolutePartitionConstraint locations, List<String> udfRecords) {
        super(locations);
        this.udfRecords = udfRecords;
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        List<String> records = new ArrayList<>();
        Map<String, List<String>> aliasIndex = buildAliasIndex();
        for (FunctionIdentifier fi : BuiltinFunctions.getBuiltinFunctionIdentifiers()) {
            records.add(toBuiltinRecord(fi, aliasIndex));
        }
        records.addAll(udfRecords);
        return new FunctionMetadataReader(records.toArray(new String[0]));
    }

    private static String toBuiltinRecord(FunctionIdentifier fi, Map<String, List<String>> aliasIndex) {
        BuiltinFunctionInfo info = BuiltinFunctions.getBuiltinFunctionInfo(fi);
        boolean isPrivate = info != null && info.isPrivate();
        List<String> aliases = aliasIndex.getOrDefault(fi.getName(), Collections.emptyList());
        return buildRecord(underscore(fi.getName()), fi.getArity(), category(fi), isPrivate, KIND_BUILTIN, null,
                aliases);
    }

    /**
     * Builds a single JSON row. Shared by builtin rows (here) and UDF rows (resolved on the CC in
     * {@link FunctionMetadataDatasource}) so both sides emit an identical schema.
     */
    static String buildRecord(String name, int arity, String category, boolean isPrivate, String kind, String dataverse,
            List<String> aliases) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"name\":\"").append(escape(name)).append("\",\"arity\":").append(arity).append(",\"category\":\"")
                .append(escape(category)).append("\",\"private\":").append(isPrivate).append(",\"kind\":\"")
                .append(escape(kind)).append("\",\"dataverse\":");
        if (dataverse == null) {
            sb.append("null");
        } else {
            sb.append('"').append(escape(dataverse)).append('"');
        }
        sb.append(",\"aliases\":[");
        for (int i = 0; i < aliases.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(escape(aliases.get(i))).append('"');
        }
        sb.append("]}");
        return sb.toString();
    }

    static String category(FunctionIdentifier fi) {
        if (BuiltinFunctions.isWindowFunction(fi) || BuiltinFunctions.getWindowFunction(fi) != null) {
            // isWindowFunction matches the internal *-impl ids; getWindowFunction matches the
            // user-facing names (row_number, rank, ...) that map to those impls.
            return "window";
        }
        if (BuiltinFunctions.isBuiltinAggregateFunction(fi)) {
            return "aggregate";
        }
        if (BuiltinFunctions.isBuiltinScalarAggregateFunction(fi)) {
            return "aggregate-scalar";
        }
        // Datasource (table-valued) functions are registered as BOTH unnest and datasource, so the
        // datasource check MUST come before the unnest check; otherwise every datasource function
        // (function_metadata, active_requests, jobs, ...) would be mislabeled "unnest".
        if (BuiltinFunctions.getDatasourceTransformer(fi) != null) {
            return "datasource";
        }
        if (BuiltinFunctions.isBuiltinUnnestingFunction(fi)) {
            return "unnest";
        }
        return "scalar";
    }

    /**
     * Builds an index from an internal (hyphenated) function name to its callable aliases, derived
     * from {@link CommonFunctionMapUtil}. Alias spellings are normalized to the underscore form so
     * they line up with the {@code name} column.
     */
    static Map<String, List<String>> buildAliasIndex() {
        Map<String, List<String>> index = new HashMap<>();
        for (Map.Entry<String, String> e : CommonFunctionMapUtil.getFunctionMappings().entrySet()) {
            index.computeIfAbsent(e.getValue(), k -> new ArrayList<>()).add(underscore(e.getKey()));
        }
        for (List<String> aliases : index.values()) {
            Collections.sort(aliases);
        }
        return index;
    }

    static String underscore(String name) {
        return name.replace('-', '_');
    }

    private static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\').append(c);
            } else if (c < 0x20) {
                sb.append(String.format("\\u%04x", (int) c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
