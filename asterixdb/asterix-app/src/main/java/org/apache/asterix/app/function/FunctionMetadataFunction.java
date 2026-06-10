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
import java.util.List;

import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.metadata.declared.AbstractDatasourceFunction;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FunctionMetadataFunction extends AbstractDatasourceFunction {

    private static final long serialVersionUID = 1L;

    public FunctionMetadataFunction(AlgebricksAbsolutePartitionConstraint locations) {
        super(locations);
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        List<String> records = new ArrayList<>();
        for (FunctionIdentifier fi : BuiltinFunctions.getBuiltinFunctionIdentifiers()) {
            records.add(toRecord(fi));
        }
        return new FunctionMetadataReader(records.toArray(new String[0]));
    }

    private static String toRecord(FunctionIdentifier fi) {
        BuiltinFunctionInfo info = BuiltinFunctions.getBuiltinFunctionInfo(fi);
        boolean isPrivate = info != null && info.isPrivate();
        return "{\"name\":\"" + escape(fi.getName()) + "\",\"arity\":" + fi.getArity() + ",\"category\":\""
                + category(fi) + "\",\"private\":" + isPrivate + "}";
    }

    private static String category(FunctionIdentifier fi) {
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
        if (BuiltinFunctions.isBuiltinUnnestingFunction(fi)) {
            return "unnest";
        }
        if (BuiltinFunctions.getDatasourceTransformer(fi) != null) {
            return "datasource";
        }
        return "scalar";
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
