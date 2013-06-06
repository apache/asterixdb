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
package edu.uci.ics.hyracks.algebricks.examples.piglet.metadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.examples.piglet.types.Type;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.SinkWriterRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class PigletMetadataProvider implements IMetadataProvider<String, String> {
    private static final Map<FunctionIdentifier, PigletFunction> FN_MAP;

    static {
        Map<FunctionIdentifier, PigletFunction> map = new HashMap<FunctionIdentifier, PigletFunction>();

        map.put(AlgebricksBuiltinFunctions.EQ, new PigletFunction(AlgebricksBuiltinFunctions.EQ));

        FN_MAP = Collections.unmodifiableMap(map);
    }

    @Override
    public IDataSource<String> findDataSource(String id) throws AlgebricksException {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(IDataSource<String> dataSource,
            List<LogicalVariable> scanVariables, List<LogicalVariable> projectVariables, boolean projectPushed,
            IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv, JobGenContext context,
            JobSpecification jobSpec, Object implConfig) throws AlgebricksException {
        PigletFileDataSource ds = (PigletFileDataSource) dataSource;

        FileSplit[] fileSplits = ds.getFileSplits();
        String[] locations = new String[fileSplits.length];
        for (int i = 0; i < fileSplits.length; ++i) {
            locations[i] = fileSplits[i].getNodeName();
        }
        IFileSplitProvider fsp = new ConstantFileSplitProvider(fileSplits);

        Object[] colTypes = ds.getSchemaTypes();
        IValueParserFactory[] vpfs = new IValueParserFactory[colTypes.length];
        ISerializerDeserializer[] serDesers = new ISerializerDeserializer[colTypes.length];

        for (int i = 0; i < colTypes.length; ++i) {
            Type colType = (Type) colTypes[i];
            IValueParserFactory vpf;
            ISerializerDeserializer serDeser;
            switch (colType.getTag()) {
                case INTEGER:
                    vpf = IntegerParserFactory.INSTANCE;
                    serDeser = IntegerSerializerDeserializer.INSTANCE;
                    break;

                case CHAR_ARRAY:
                    vpf = UTF8StringParserFactory.INSTANCE;
                    serDeser = UTF8StringSerializerDeserializer.INSTANCE;
                    break;

                case FLOAT:
                    vpf = FloatParserFactory.INSTANCE;
                    serDeser = FloatSerializerDeserializer.INSTANCE;
                    break;

                default:
                    throw new UnsupportedOperationException();
            }
            vpfs[i] = vpf;
            serDesers[i] = serDeser;
        }

        ITupleParserFactory tpf = new DelimitedDataTupleParserFactory(vpfs, ',');
        RecordDescriptor rDesc = new RecordDescriptor(serDesers);

        IOperatorDescriptor scanner = new FileScanOperatorDescriptor(jobSpec, fsp, tpf, rDesc);
        AlgebricksAbsolutePartitionConstraint constraint = new AlgebricksAbsolutePartitionConstraint(locations);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(scanner, constraint);
    }

    @Override
    public boolean scannerOperatorIsLeaf(IDataSource<String> dataSource) {
        return true;
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc)
            throws AlgebricksException {
        PigletFileDataSink ds = (PigletFileDataSink) sink;
        FileSplit[] fileSplits = ds.getFileSplits();
        String[] locations = new String[fileSplits.length];
        for (int i = 0; i < fileSplits.length; ++i) {
            locations[i] = fileSplits[i].getNodeName();
        }
        IPushRuntimeFactory prf = new SinkWriterRuntimeFactory(printColumns, printerFactories, fileSplits[0]
                .getLocalFile().getFile(), PrinterBasedWriterFactory.INSTANCE, inputDesc);
        AlgebricksAbsolutePartitionConstraint constraint = new AlgebricksAbsolutePartitionConstraint(locations);
        return new Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint>(prf, constraint);
    }

    @Override
    public IDataSourceIndex<String, String> findDataSourceIndex(String indexId, String dataSourceId)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getResultHandleRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc, boolean ordered,
            JobSpecification spec) throws AlgebricksException {
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
            IDataSource<String> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, JobGenContext context, JobSpecification jobSpec) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, String> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, String> dataSource, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return FN_MAP.get(fid);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(IDataSource<String> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, RecordDescriptor recordDesc, JobGenContext context, JobSpecification jobSpec)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(IDataSource<String> dataSource,
            IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv, List<LogicalVariable> keys,
            LogicalVariable payLoadVar, RecordDescriptor recordDesc, JobGenContext context, JobSpecification jobSpec)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

}
