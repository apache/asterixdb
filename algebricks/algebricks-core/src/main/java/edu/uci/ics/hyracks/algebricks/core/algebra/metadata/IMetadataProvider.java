/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.metadata;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public interface IMetadataProvider<S, I> {
    public IDataSource<S> findDataSource(S id) throws AlgebricksException;

    /**
     * Obs: A scanner may choose to contribute a null
     * AlgebricksPartitionConstraint and implement
     * contributeSchedulingConstraints instead.
     */
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(IDataSource<S> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv, JobGenContext context,
            JobSpecification jobSpec, Object implConfig) throws AlgebricksException;

    public boolean scannerOperatorIsLeaf(IDataSource<S> dataSource);

    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc)
            throws AlgebricksException;

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getResultHandleRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc, boolean ordered,
            JobSpecification spec) throws AlgebricksException;

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(IDataSource<S> dataSource,
            IOperatorSchema propagatedSchema, List<LogicalVariable> keys, LogicalVariable payLoadVar,
            JobGenContext context, JobSpecification jobSpec) throws AlgebricksException;

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(IDataSource<S> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, RecordDescriptor recordDesc, JobGenContext context, JobSpecification jobSpec)
            throws AlgebricksException;

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(IDataSource<S> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, RecordDescriptor recordDesc, JobGenContext context, JobSpecification jobSpec)
            throws AlgebricksException;

    /**
     * Creates the insert runtime of IndexInsertDeletePOperator, which models
     * insert/delete operations into a secondary index.
     * 
     * @param dataSource
     *            Target secondary index.
     * @param propagatedSchema
     *            Output schema of the insert/delete operator to be created.
     * @param inputSchemas
     *            Output schemas of the insert/delete operator to be created.
     * @param typeEnv
     *            Type environment of the original IndexInsertDeleteOperator operator.
     * @param primaryKeys
     *            Variables for the dataset's primary keys that the dataSource secondary index belongs to.
     * @param secondaryKeys
     *            Variables for the secondary-index keys.
     * @param filterExpr
     *            Filtering expression to be pushed inside the runtime op.
     *            Such a filter may, e.g., exclude NULLs from being inserted/deleted.
     * @param recordDesc
     *            Output record descriptor of the runtime op to be created.
     * @param context
     *            Job generation context.
     * @param spec
     *            Target job specification.
     * @return
     *         A Hyracks IOperatorDescriptor and its partition constraint.
     * @throws AlgebricksException
     */
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<I, S> dataSource, IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas,
            IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec)
            throws AlgebricksException;

    /**
     * Creates the delete runtime of IndexInsertDeletePOperator, which models
     * insert/delete operations into a secondary index.
     * 
     * @param dataSource
     *            Target secondary index.
     * @param propagatedSchema
     *            Output schema of the insert/delete operator to be created.
     * @param inputSchemas
     *            Output schemas of the insert/delete operator to be created.
     * @param typeEnv
     *            Type environment of the original IndexInsertDeleteOperator operator.
     * @param primaryKeys
     *            Variables for the dataset's primary keys that the dataSource secondary index belongs to.
     * @param secondaryKeys
     *            Variables for the secondary-index keys.
     * @param filterExpr
     *            Filtering expression to be pushed inside the runtime op.
     *            Such a filter may, e.g., exclude NULLs from being inserted/deleted.
     * @param recordDesc
     *            Output record descriptor of the runtime op to be created.
     * @param context
     *            Job generation context.
     * @param spec
     *            Target job specification.
     * @return
     *         A Hyracks IOperatorDescriptor and its partition constraint.
     * @throws AlgebricksException
     */
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<I, S> dataSource, IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas,
            IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec)
            throws AlgebricksException;

    public IDataSourceIndex<I, S> findDataSourceIndex(I indexId, S dataSourceId) throws AlgebricksException;

    public IFunctionInfo lookupFunction(FunctionIdentifier fid);

}
