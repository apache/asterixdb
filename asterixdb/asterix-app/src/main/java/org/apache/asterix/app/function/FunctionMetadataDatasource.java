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
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.IDatasourceFunction;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class FunctionMetadataDatasource extends FunctionDataSource {

    private static final DataSourceId FUNCTION_METADATA_DATASOURCE_ID =
            createDataSourceId(FunctionMetadataRewriter.FUNCTION_METADATA);

    public FunctionMetadataDatasource(INodeDomain domain) throws AlgebricksException {
        super(FUNCTION_METADATA_DATASOURCE_ID, FunctionMetadataRewriter.FUNCTION_METADATA, domain);
    }

    @Override
    protected IDatasourceFunction createFunction(MetadataProvider metadataProvider,
            AlgebricksAbsolutePartitionConstraint locations) throws AlgebricksException {
        // UDFs live in the metadata catalog (only reachable from the CC), so resolve them here and
        // ship the pre-built rows to the node. Builtins are read from the static registry on the node.
        List<String> udfRecords = collectUdfRecords(metadataProvider);
        // The builtin function registry is identical on every node, so run on a single
        // location to avoid emitting duplicate rows.
        return new FunctionMetadataFunction(
                AlgebricksAbsolutePartitionConstraint.randomLocation(locations.getLocations()), udfRecords);
    }

    private static List<String> collectUdfRecords(MetadataProvider metadataProvider) throws AlgebricksException {
        MetadataTransactionContext mdTxnCtx = metadataProvider.getMetadataTxnContext();
        List<String> records = new ArrayList<>();
        if (mdTxnCtx == null) {
            return records;
        }
        for (Dataverse dv : MetadataManager.INSTANCE.getDataverses(mdTxnCtx)) {
            List<Function> functions = MetadataManager.INSTANCE.getDataverseFunctions(mdTxnCtx, dv.getDatabaseName(),
                    dv.getDataverseName());
            for (Function fn : functions) {
                String category = fn.getKind() == null ? "scalar" : fn.getKind().toLowerCase(Locale.ROOT);
                records.add(FunctionMetadataFunction.buildRecord(fn.getName(), fn.getArity(), category, false,
                        FunctionMetadataFunction.KIND_UDF, fn.getDataverseName().toString(), Collections.emptyList()));
            }
        }
        return records;
    }

    @Override
    protected boolean sameFunctionDatasource(FunctionDataSource other) {
        return Objects.equals(this.functionId, other.getFunctionId());
    }
}
