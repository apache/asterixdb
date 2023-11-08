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

import static org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier.VARARGS;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class DatasetResourcesRewriter extends FunctionRewriter {

    // Parameters are dataverse name, dataset name, and database name
    public static final FunctionIdentifier DATASET_RESOURCES =
            FunctionConstants.newAsterix("dataset-resources", VARARGS);
    public static final DatasetResourcesRewriter INSTANCE = new DatasetResourcesRewriter(DATASET_RESOURCES);

    private DatasetResourcesRewriter(FunctionIdentifier functionId) {
        super(functionId);
    }

    @Override
    public DatasetResourcesDatasource toDatasource(IOptimizationContext context, AbstractFunctionCallExpression f)
            throws AlgebricksException {
        final SourceLocation loc = f.getSourceLocation();
        DataverseName dataverseName = getDataverseName(loc, f.getArguments(), 0);
        String datasetName = getString(loc, f.getArguments(), 1);
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        String database;
        if (f.getArguments().size() > 2) {
            database = getString(loc, f.getArguments(), 2);
        } else {
            database = MetadataUtil.databaseFor(dataverseName);
        }
        Dataset dataset = metadataProvider.findDataset(database, dataverseName, datasetName);
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, loc, datasetName,
                    MetadataUtil.dataverseName(database, dataverseName, metadataProvider.isUsingDatabase()));
        }
        return new DatasetResourcesDatasource(context.getComputationNodeDomain(), dataset.getDatasetId());
    }

    @Override
    protected boolean invalidArgs(List<Mutable<ILogicalExpression>> args) {
        return args.size() < 2;
    }
}
