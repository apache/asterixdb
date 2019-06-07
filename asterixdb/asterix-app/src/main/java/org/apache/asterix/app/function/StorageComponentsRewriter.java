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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class StorageComponentsRewriter extends FunctionRewriter {

    // Parameters are dataverse name, and dataset name
    public static final FunctionIdentifier STORAGE_COMPONENTS =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "storage-components", 2);
    public static final StorageComponentsRewriter INSTANCE = new StorageComponentsRewriter(STORAGE_COMPONENTS);

    private StorageComponentsRewriter(FunctionIdentifier functionId) {
        super(functionId);
    }

    @Override
    public StorageComponentsDatasource toDatasource(IOptimizationContext context, AbstractFunctionCallExpression f)
            throws AlgebricksException {
        SourceLocation loc = f.getSourceLocation();
        String dataverseName = getString(loc, f.getArguments(), 0);
        String datasetName = getString(loc, f.getArguments(), 1);
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, loc, datasetName, dataverseName);
        }
        return new StorageComponentsDatasource(context.getComputationNodeDomain(), dataset.getDatasetId());
    }
}
