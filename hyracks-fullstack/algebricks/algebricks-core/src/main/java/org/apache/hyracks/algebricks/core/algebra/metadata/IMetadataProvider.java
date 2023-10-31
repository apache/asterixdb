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
package org.apache.hyracks.algebricks.core.algebra.metadata;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

public interface IMetadataProvider<S, I> {

    /**
     * Obs: A scanner may choose to contribute a null
     * AlgebricksPartitionConstraint and implement
     * contributeSchedulingConstraints instead.
     */
    Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(IDataSource<S> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            List<LogicalVariable> minFilterVars, List<LogicalVariable> maxFilterVars,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig,
            IProjectionFiltrationInfo projectionFiltrationInfo) throws AlgebricksException;

    Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(int sourceColumn,
            int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IScalarEvaluatorFactory dynamicPathEvalFactory, ILogicalExpression staticPathExpr,
            SourceLocation pathSourceLocation, IWriteDataSink sink, RecordDescriptor inputDesc, Object sourceType)
            throws AlgebricksException;

    Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getResultHandleRuntime(IDataSink sink, int[] printColumns,
            IPrinterFactory[] printerFactories, IAWriterFactory writerFactory,
            IResultSerializerFactoryProvider resultSerializerFactoryProvider, RecordDescriptor inputDesc,
            IResultMetadata metadata, JobSpecification spec) throws AlgebricksException;

    Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(IDataSource<S> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, List<LogicalVariable> additionalFilterKeyFields,
            List<LogicalVariable> additionalNonFilteringFields, RecordDescriptor inputRecordDesc, JobGenContext context,
            JobSpecification jobSpec, boolean bulkload) throws AlgebricksException;

    Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getUpsertRuntime(IDataSource<S> dataSource,
            IOperatorSchema inputSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, List<LogicalVariable> additionalFilterFields,
            List<LogicalVariable> additionalNonFilteringFields, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification jobSpec) throws AlgebricksException;

    Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(IDataSource<S> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, List<LogicalVariable> additionalNonKeyFields,
            List<LogicalVariable> additionalNonFilteringFields, RecordDescriptor inputRecordDesc, JobGenContext context,
            JobSpecification jobSpec) throws AlgebricksException;

    /**
     * Creates the insert runtime of IndexInsertDeletePOperator, which models
     * insert/delete operations into a secondary index.
     *
     * @param dataSource             Target secondary index.
     * @param propagatedSchema       Output schema of the insert/delete operator to be created.
     * @param inputSchemas           Output schemas of the insert/delete operator to be created.
     * @param typeEnv                Type environment of the original IndexInsertDeleteOperator operator.
     * @param primaryKeys            Variables for the dataset's primary keys that the dataSource secondary index belongs to.
     * @param secondaryKeys          Variables for the secondary-index keys.
     * @param additionalNonKeyFields Additional variables that can be passed to the secondary index as payload.
     *                               This can be useful when creating a second filter on a non-primary and non-secondary
     *                               fields for additional pruning power.
     * @param filterExpr             Filtering expression to be pushed inside the runtime op.
     *                               Such a filter may, e.g., exclude NULLs from being inserted/deleted.
     * @param recordDesc             Output record descriptor of the runtime op to be created.
     * @param context                Job generation context.
     * @param spec                   Target job specification.
     * @param secondaryKeysPipelines Nested plans to extract secondary keys.
     * @param pipelineTopSchema      Schema of the primary pipeline for secondary keys.
     * @return A Hyracks IOperatorDescriptor and its partition constraint.
     * @throws AlgebricksException
     */
    Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(IDataSourceIndex<I, S> dataSource,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalNonKeyFields, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec, boolean bulkload,
            List<List<AlgebricksPipeline>> secondaryKeysPipelines, IOperatorSchema pipelineTopSchema)
            throws AlgebricksException;

    Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexUpsertRuntime(
            IDataSourceIndex<I, S> dataSourceIndex, IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas,
            IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalFilteringKeys, ILogicalExpression filterExpr,
            ILogicalExpression prevFilterExpr, LogicalVariable operationVar, List<LogicalVariable> prevSecondaryKeys,
            LogicalVariable prevAdditionalFilteringKeys, RecordDescriptor inputDesc, JobGenContext context,
            JobSpecification spec, List<List<AlgebricksPipeline>> secondaryKeysPipelines) throws AlgebricksException;

    /**
     * Creates the delete runtime of IndexInsertDeletePOperator, which models
     * insert/delete operations into a secondary index.
     *
     * @param dataSource             Target secondary index.
     * @param propagatedSchema       Output schema of the insert/delete operator to be created.
     * @param inputSchemas           Output schemas of the insert/delete operator to be created.
     * @param typeEnv                Type environment of the original IndexInsertDeleteOperator operator.
     * @param primaryKeys            Variables for the dataset's primary keys that the dataSource secondary index belongs to.
     * @param secondaryKeys          Variables for the secondary-index keys.
     * @param additionalNonKeyFields Additional variables that can be passed to the secondary index as payload.
     *                               This can be useful when creating a second filter on a non-primary and non-secondary
     *                               fields for additional pruning power.
     * @param filterExpr             Filtering expression to be pushed inside the runtime op.
     *                               Such a filter may, e.g., exclude NULLs from being inserted/deleted.
     * @param recordDesc             Output record descriptor of the runtime op to be created.
     * @param context                Job generation context.
     * @param spec                   Target job specification.
     * @param secondaryKeysPipelines Nested plan to extract secondary keys.
     * @param pipelineTopSchema      Schema of the primary pipeline for secondary keys.
     * @return A Hyracks IOperatorDescriptor and its partition constraint.
     * @throws AlgebricksException
     */
    Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(IDataSourceIndex<I, S> dataSource,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            List<LogicalVariable> additionalNonKeyFields, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec, List<List<AlgebricksPipeline>> secondaryKeysPipelines,
            IOperatorSchema pipelineTopSchema) throws AlgebricksException;

    /**
     * Creates the TokenizeOperator for IndexInsertDeletePOperator, which tokenizes
     * secondary key into [token, number of token] pair in a length-partitioned index.
     * In case of non length-partitioned index, it tokenizes secondary key into [token].
     *
     * @param dataSource       Target secondary index.
     * @param propagatedSchema Output schema of the insert/delete operator to be created.
     * @param inputSchemas     Output schemas of the insert/delete operator to be created.
     * @param typeEnv          Type environment of the original IndexInsertDeleteOperator operator.
     * @param primaryKeys      Variables for the dataset's primary keys that the dataSource secondary index belongs to.
     * @param secondaryKeys    Variables for the secondary-index keys.
     * @param filterExpr       Filtering expression to be pushed inside the runtime op.
     *                         Such a filter may, e.g., exclude NULLs from being inserted/deleted.
     * @param recordDesc       Output record descriptor of the runtime op to be created.
     * @param context          Job generation context.
     * @param spec             Target job specification.
     * @return A Hyracks IOperatorDescriptor and its partition constraint.
     * @throws AlgebricksException
     */
    Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getTokenizerRuntime(IDataSourceIndex<I, S> dataSource,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec, boolean bulkload)
            throws AlgebricksException;

    IDataSource<S> findDataSource(S id) throws AlgebricksException;

    IDataSourceIndex<I, S> findDataSourceIndex(I indexId, S dataSourceId) throws AlgebricksException;

    IFunctionInfo lookupFunction(FunctionIdentifier fid);

    ITupleFilterFactory createTupleFilterFactory(IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv,
            ILogicalExpression filterExpr, JobGenContext context) throws AlgebricksException;

    Map<String, Object> getConfig();

    boolean isBlockingOperatorDisabled();

}
