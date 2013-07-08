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
package edu.uci.ics.hivesterix.runtime.jobgen;

import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;

import edu.uci.ics.hivesterix.logical.expression.HiveFunctionInfo;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;

@SuppressWarnings("rawtypes")
public class HiveMetaDataProvider<S, T> implements IMetadataProvider<S, T> {

    private Operator fileSink;
    private Schema outputSchema;
    private HashMap<S, IDataSource<S>> dataSourceMap;

    public HiveMetaDataProvider(Operator fsOp, Schema oi, HashMap<S, IDataSource<S>> map) {
        fileSink = fsOp;
        outputSchema = oi;
        dataSourceMap = map;
    }

    @Override
    public IDataSourceIndex<T, S> findDataSourceIndex(T indexId, S dataSourceId) throws AlgebricksException {
        return null;
    }

    @Override
    public IDataSource<S> findDataSource(S id) throws AlgebricksException {
        return dataSourceMap.get(id);
    }

    @Override
    public boolean scannerOperatorIsLeaf(IDataSource<S> dataSource) {
        return true;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(IDataSource<S> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv, JobGenContext context,
            JobSpecification jobSpec, Object implConfig) throws AlgebricksException {

        S desc = dataSource.getId();
        HiveScanRuntimeGenerator generator = new HiveScanRuntimeGenerator((PartitionDesc) desc);
        return generator.getRuntimeOperatorAndConstraint(dataSource, scanVariables, projectVariables, projectPushed,
                context, jobSpec);
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc) {

        HiveWriteRuntimeGenerator generator = new HiveWriteRuntimeGenerator((FileSinkOperator) fileSink, outputSchema);
        return generator.getWriterRuntime(inputDesc);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(IDataSource<S> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, RecordDescriptor recordDesc, JobGenContext context, JobSpecification jobSpec)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(IDataSource<S> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, RecordDescriptor recordDesc, JobGenContext context, JobSpecification jobSpec)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getResultHandleRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc, boolean ordered,
            JobSpecification spec) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(IDataSource<S> arg0,
            IOperatorSchema arg1, List<LogicalVariable> arg2, LogicalVariable arg3, JobGenContext arg4,
            JobSpecification arg5) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IFunctionInfo lookupFunction(FunctionIdentifier arg0) {
        return new HiveFunctionInfo(arg0, null);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<T, S> dataSource, IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas,
            IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<T, S> dataSource, IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas,
            IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            ILogicalExpression filterExpr, RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }
}
