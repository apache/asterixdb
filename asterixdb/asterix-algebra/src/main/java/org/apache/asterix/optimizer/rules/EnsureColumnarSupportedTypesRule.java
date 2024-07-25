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
package org.apache.asterix.optimizer.rules;

import org.apache.asterix.column.validation.ColumnSupportedTypesValidator;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * This rule enforces that inserted or upserted records do not contain unsupported
 * types for {@link DatasetConfig.DatasetFormat#COLUMN} datasets.
 */
public class EnsureColumnarSupportedTypesRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE_UPSERT || context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        InsertDeleteUpsertOperator modOp = (InsertDeleteUpsertOperator) op;
        // Do not apply the rule again to this operator
        context.addToDontApplySet(this, op);

        InsertDeleteUpsertOperator.Kind operation = modOp.getOperation();
        if (operation != InsertDeleteUpsertOperator.Kind.INSERT
                && operation != InsertDeleteUpsertOperator.Kind.UPSERT) {
            return false;
        }

        DatasetConfig.DatasetFormat format = getFormat(modOp, context);
        if (format != DatasetConfig.DatasetFormat.COLUMN) {
            return false;
        }

        IVariableTypeEnvironment typeEnv = context.getOutputTypeEnvironment(modOp);
        IAType type = (IAType) typeEnv.getType(modOp.getPayloadExpression().getValue());
        SourceLocation srcLoc = modOp.getPayloadExpression().getValue().getSourceLocation();
        ColumnSupportedTypesValidator.validate(format, type, srcLoc);

        return false;
    }

    private DatasetConfig.DatasetFormat getFormat(InsertDeleteUpsertOperator modOp, IOptimizationContext context)
            throws AlgebricksException {
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        DataSource dataSource = (DataSource) modOp.getDataSource();
        if (dataSource == null) {
            return null;
        }
        DataverseName dataverse = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasourceName();
        String database = dataSource.getId().getDatabaseName();
        Dataset dataset = metadataProvider.findDataset(database, dataverse, datasetName);
        if (dataset != null) {
            return dataset.getDatasetFormatInfo().getFormat();
        }

        return null;
    }
}
