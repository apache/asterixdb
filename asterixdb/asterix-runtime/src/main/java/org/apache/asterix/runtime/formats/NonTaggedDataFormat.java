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
package org.apache.asterix.runtime.formats;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.AsterixRuntimeException;
import org.apache.asterix.dataflow.data.nontagged.AqlNullWriterFactory;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.formats.nontagged.AqlADMPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlBinaryBooleanInspectorImpl;
import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlBinaryHashFunctionFamilyProvider;
import org.apache.asterix.formats.nontagged.AqlBinaryIntegerInspector;
import org.apache.asterix.formats.nontagged.AqlCSVPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlCleanJSONPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlLosslessJSONPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlNormalizedKeyComputerFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlPredicateEvaluatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.AqlTypeTraitProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.FunctionManagerHolder;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.TypeComputerUtilities;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.collections.ListifyAggregateDescriptor;
import org.apache.asterix.runtime.evaluators.common.CreateMBREvalFactory;
import org.apache.asterix.runtime.evaluators.common.FunctionManagerImpl;
import org.apache.asterix.runtime.evaluators.constructors.ClosedRecordConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.OpenRecordConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CastListDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CastRecordDescriptor;
import org.apache.asterix.runtime.evaluators.functions.DeepEqualityDescriptor;
import org.apache.asterix.runtime.evaluators.functions.FlowRecordDescriptor;
import org.apache.asterix.runtime.evaluators.functions.OrderedListConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.UnorderedListConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessByIndexDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessByIndexEvalFactory;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessByNameDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessNestedDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessNestedEvalFactory;
import org.apache.asterix.runtime.evaluators.functions.records.GetRecordFieldValueDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.GetRecordFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordAddFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordMergeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordRemoveFieldsDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableEvalSizeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import org.apache.hyracks.algebricks.data.IPrinterFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.LongParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;

public class NonTaggedDataFormat implements IDataFormat {

    private static boolean registered = false;

    public static final NonTaggedDataFormat INSTANCE = new NonTaggedDataFormat();

    private static LogicalVariable METADATA_DUMMY_VAR = new LogicalVariable(-1);

    private static final HashMap<ATypeTag, IValueParserFactory> typeToValueParserFactMap = new HashMap<>();

    public static final String NON_TAGGED_DATA_FORMAT = "org.apache.asterix.runtime.formats.NonTaggedDataFormat";

    private Map<FunctionIdentifier, FunctionTypeInferer> functionTypeInferers = new HashMap<>();

    static {
        typeToValueParserFactMap.put(ATypeTag.INT32, IntegerParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.INT64, LongParserFactory.INSTANCE);
        typeToValueParserFactMap.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
    }

    public NonTaggedDataFormat() {
    }

    @Override
    public void registerRuntimeFunctions(List<IFunctionDescriptorFactory> funcDescriptors) throws AlgebricksException {

        if (registered) {
            return;
        }
        registered = true;

        if (FunctionManagerHolder.getFunctionManager() != null) {
            return;
        }

        IFunctionManager mgr = new FunctionManagerImpl();
        for (IFunctionDescriptorFactory fdFactory : funcDescriptors) {
            mgr.registerFunction(fdFactory);
        }
        FunctionManagerHolder.setFunctionManager(mgr);

        registerTypeInferers();
    }

    @Override
    public IBinaryBooleanInspectorFactory getBinaryBooleanInspectorFactory() {
        return AqlBinaryBooleanInspectorImpl.FACTORY;
    }

    @Override
    public IBinaryComparatorFactoryProvider getBinaryComparatorFactoryProvider() {
        return AqlBinaryComparatorFactoryProvider.INSTANCE;
    }

    @Override
    public IBinaryHashFunctionFactoryProvider getBinaryHashFunctionFactoryProvider() {
        return AqlBinaryHashFunctionFactoryProvider.INSTANCE;
    }

    @Override
    public ISerializerDeserializerProvider getSerdeProvider() {
        return AqlSerializerDeserializerProvider.INSTANCE; // done
    }

    @Override
    public ITypeTraitProvider getTypeTraitProvider() {
        return AqlTypeTraitProvider.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IScalarEvaluatorFactory getFieldAccessEvaluatorFactory(ARecordType recType, List<String> fldName,
            int recordColumn) throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        boolean fieldFound = false;
        IScalarEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(recordColumn);
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        DataOutput dos = abvs.getDataOutput();
        IScalarEvaluatorFactory evalFactory = null;
        if (fldName.size() == 1) {
            for (int i = 0; i < n; i++) {
                if (names[i].equals(fldName.get(0))) {
                    fieldFound = true;
                    try {
                        AInt32 ai = new AInt32(i);
                        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai,
                                dos);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    IScalarEvaluatorFactory fldIndexEvalFactory = new ConstantEvalFactory(
                            Arrays.copyOf(abvs.getByteArray(), abvs.getLength()));

                    evalFactory = new FieldAccessByIndexEvalFactory(recordEvalFactory, fldIndexEvalFactory, recType);
                    return evalFactory;
                }
            }
        }
        if (fldName.size() > 1 || (!fieldFound && recType.isOpen())) {
            if (fldName.size() == 1) {
                AString as = new AString(fldName.get(0));
                try {
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as,
                            dos);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            } else {
                AOrderedList as = new AOrderedList(fldName);
                try {
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as,
                            dos);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }
            IScalarEvaluatorFactory[] factories = new IScalarEvaluatorFactory[2];
            factories[0] = recordEvalFactory;
            if (fldName.size() > 1) {
                evalFactory = new FieldAccessNestedEvalFactory(recordEvalFactory, recType, fldName);
            } else {
                evalFactory = FieldAccessByNameDescriptor.FACTORY.createFunctionDescriptor()
                        .createEvaluatorFactory(factories);
            }
            return evalFactory;
        } else {
            throw new AlgebricksException("Could not find field " + fldName + " in the schema.");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public IScalarEvaluatorFactory[] createMBRFactory(ARecordType recType, List<String> fldName, int recordColumn,
            int dimension, List<String> filterFieldName) throws AlgebricksException {
        IScalarEvaluatorFactory evalFactory = getFieldAccessEvaluatorFactory(recType, fldName, recordColumn);
        int numOfFields = dimension * 2;
        IScalarEvaluatorFactory[] evalFactories = new IScalarEvaluatorFactory[numOfFields
                + (filterFieldName == null ? 0 : 1)];

        ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();
        DataOutput dos1 = abvs1.getDataOutput();
        try {
            AInt32 ai = new AInt32(dimension);
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai, dos1);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        IScalarEvaluatorFactory dimensionEvalFactory = new ConstantEvalFactory(
                Arrays.copyOf(abvs1.getByteArray(), abvs1.getLength()));

        for (int i = 0; i < numOfFields; i++) {
            ArrayBackedValueStorage abvs2 = new ArrayBackedValueStorage();
            DataOutput dos2 = abvs2.getDataOutput();
            try {
                AInt32 ai = new AInt32(i);
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai, dos2);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            IScalarEvaluatorFactory coordinateEvalFactory = new ConstantEvalFactory(
                    Arrays.copyOf(abvs2.getByteArray(), abvs2.getLength()));

            evalFactories[i] = new CreateMBREvalFactory(evalFactory, dimensionEvalFactory, coordinateEvalFactory);
        }
        if (filterFieldName != null) {
            evalFactories[numOfFields] = getFieldAccessEvaluatorFactory(recType, filterFieldName, recordColumn);
        }
        return evalFactories;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Triple<IScalarEvaluatorFactory, ScalarFunctionCallExpression, IAType> partitioningEvaluatorFactory(
            ARecordType recType, List<String> fldName) throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        if (fldName.size() > 1) {
            for (int i = 0; i < n; i++) {
                if (names[i].equals(fldName.get(0))) {
                    IScalarEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(
                            GlobalConfig.DEFAULT_INPUT_DATA_COLUMN);
                    ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
                    DataOutput dos = abvs.getDataOutput();
                    try {
                        AInt32 ai = new AInt32(i);
                        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai,
                                dos);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    IScalarEvaluatorFactory fldIndexEvalFactory = new ConstantEvalFactory(
                            Arrays.copyOf(abvs.getByteArray(), abvs.getLength()));
                    IScalarEvaluatorFactory evalFactory = new FieldAccessByIndexEvalFactory(recordEvalFactory,
                            fldIndexEvalFactory, recType);
                    IFunctionInfo finfoAccess = AsterixBuiltinFunctions
                            .getAsterixFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX);

                    ScalarFunctionCallExpression partitionFun = new ScalarFunctionCallExpression(finfoAccess,
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(METADATA_DUMMY_VAR)),
                            new MutableObject<ILogicalExpression>(
                                    new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
                    return new Triple<IScalarEvaluatorFactory, ScalarFunctionCallExpression, IAType>(evalFactory,
                            partitionFun, recType.getFieldTypes()[i]);
                }
            }
        } else {
            IScalarEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(
                    GlobalConfig.DEFAULT_INPUT_DATA_COLUMN);
            ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            DataOutput dos = abvs.getDataOutput();
            AOrderedList as = new AOrderedList(fldName);
            try {
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as, dos);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            IScalarEvaluatorFactory evalFactory = new FieldAccessNestedEvalFactory(recordEvalFactory, recType, fldName);
            IFunctionInfo finfoAccess = AsterixBuiltinFunctions
                    .getAsterixFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_NESTED);

            ScalarFunctionCallExpression partitionFun = new ScalarFunctionCallExpression(finfoAccess,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(METADATA_DUMMY_VAR)),
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(as))));
            return new Triple<IScalarEvaluatorFactory, ScalarFunctionCallExpression, IAType>(evalFactory, partitionFun,
                    recType.getSubFieldType(fldName));
        }
        throw new AlgebricksException("Could not find field " + fldName + " in the schema.");
    }

    @Override
    public IFunctionDescriptor resolveFunction(ILogicalExpression expr, IVariableTypeEnvironment context)
            throws AlgebricksException {
        FunctionIdentifier fnId = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier();
        IFunctionManager mgr = FunctionManagerHolder.getFunctionManager();
        IFunctionDescriptor fd = mgr.lookupFunction(fnId);
        if (fd == null) {
            throw new AsterixRuntimeException("Unresolved function " + fnId);
        }
        final FunctionIdentifier fid = fd.getIdentifier();
        if (functionTypeInferers.containsKey(fid)) {
            functionTypeInferers.get(fid).infer(expr, fd, context);
        }
        return fd;
    }

    interface FunctionTypeInferer {
        void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                throws AlgebricksException;
    }

    void registerTypeInferers() {
        functionTypeInferers.put(AsterixBuiltinFunctions.LISTIFY, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                if (f.getArguments().size() == 0) {
                    ((ListifyAggregateDescriptor) fd).reset(new AOrderedListType(null, null));
                } else {
                    IAType itemType = (IAType) context.getType(f.getArguments().get(0).getValue());
                    if (itemType instanceof AUnionType) {
                        if (((AUnionType) itemType).isNullableType()) {
                            itemType = ((AUnionType) itemType).getNullableType();
                        } else {
                            // Convert UNION types into ANY.
                            itemType = BuiltinType.ANY;
                        }
                    }
                    ((ListifyAggregateDescriptor) fd).reset(new AOrderedListType(itemType, null));
                }
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.RECORD_MERGE, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                IAType outType = (IAType) context.getType(expr);
                IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
                IAType type1 = (IAType) context.getType(f.getArguments().get(1).getValue());
                ((RecordMergeDescriptor) fd).reset(outType, type0, type1);
            }
        });

        functionTypeInferers.put(AsterixBuiltinFunctions.DEEP_EQUAL, new FunctionTypeInferer() {

            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
                IAType type1 = (IAType) context.getType(f.getArguments().get(1).getValue());
                ((DeepEqualityDescriptor) fd).reset(type0, type1);
            }
        });

        functionTypeInferers.put(AsterixBuiltinFunctions.ADD_FIELDS, new FunctionTypeInferer() {

            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                IAType outType = (IAType) context.getType(expr);
                IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
                ILogicalExpression listExpr = f.getArguments().get(1).getValue();
                IAType type1 = (IAType) context.getType(listExpr);
                if (type0.getTypeTag().equals(ATypeTag.ANY)) {
                    type0 = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                }
                if (type1.getTypeTag().equals(ATypeTag.ANY)) {
                    type1 = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
                }
                ((RecordAddFieldsDescriptor) fd).reset(outType, type0, type1);
            }
        });

        functionTypeInferers.put(AsterixBuiltinFunctions.REMOVE_FIELDS, new FunctionTypeInferer() {

            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                IAType outType = (IAType) context.getType(expr);
                IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
                ILogicalExpression le = f.getArguments().get(1).getValue();
                IAType type1 = (IAType) context.getType(le);
                if (type0.getTypeTag().equals(ATypeTag.ANY)) {
                    type0 = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                }
                if (type1.getTypeTag().equals(ATypeTag.ANY)) {
                    type1 = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
                }
                ((RecordRemoveFieldsDescriptor) fd).reset(outType, type0, type1);
            }
        });

        functionTypeInferers.put(AsterixBuiltinFunctions.CAST_RECORD, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
                ARecordType rt = (ARecordType) TypeComputerUtilities.getRequiredType(funcExpr);
                IAType it = (IAType) context.getType(funcExpr.getArguments().get(0).getValue());
                if (it.getTypeTag().equals(ATypeTag.ANY)) {
                    it = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                }
                ((CastRecordDescriptor) fd).reset(rt, (ARecordType) it);
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.CAST_LIST, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
                AbstractCollectionType rt = (AbstractCollectionType) TypeComputerUtilities.getRequiredType(funcExpr);
                IAType it = (IAType) context.getType(funcExpr.getArguments().get(0).getValue());
                if (it.getTypeTag().equals(ATypeTag.ANY)) {
                    it = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
                }
                ((CastListDescriptor) fd).reset(rt, (AbstractCollectionType) it);
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.FLOW_RECORD, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                ARecordType it = (ARecordType) TypeComputerUtilities
                        .getInputType((AbstractFunctionCallExpression) expr);
                ((FlowRecordDescriptor) fd).reset(it);
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                ARecordType rt = (ARecordType) context.getType(expr);
                ((OpenRecordConstructorDescriptor) fd).reset(rt,
                        computeOpenFields((AbstractFunctionCallExpression) expr, rt));
            }

            private boolean[] computeOpenFields(AbstractFunctionCallExpression expr, ARecordType recType) {
                int n = expr.getArguments().size() / 2;
                boolean[] open = new boolean[n];
                for (int i = 0; i < n; i++) {
                    Mutable<ILogicalExpression> argRef = expr.getArguments().get(2 * i);
                    ILogicalExpression arg = argRef.getValue();
                    if (arg.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                        String fn = ((AString) ((AsterixConstantValue) ((ConstantExpression) arg).getValue())
                                .getObject()).getStringValue();
                        open[i] = true;
                        for (String s : recType.getFieldNames()) {
                            if (s.equals(fn)) {
                                open[i] = false;
                                break;
                            }
                        }
                    } else {
                        open[i] = true;
                    }
                }
                return open;
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                ((ClosedRecordConstructorDescriptor) fd).reset((ARecordType) context.getType(expr));
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                ((OrderedListConstructorDescriptor) fd).reset((AOrderedListType) context.getType(expr));
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                ((UnorderedListConstructorDescriptor) fd).reset((AUnorderedListType) context.getType(expr));
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
                switch (t.getTypeTag()) {
                    case RECORD: {
                        ARecordType recType = (ARecordType) t;
                        ((FieldAccessByIndexDescriptor) fd).reset(recType);
                        break;
                    }
                    case UNION: {
                        AUnionType unionT = (AUnionType) t;
                        if (unionT.isNullableType()) {
                            IAType t2 = unionT.getNullableType();
                            if (t2.getTypeTag() == ATypeTag.RECORD) {
                                ARecordType recType = (ARecordType) t2;
                                ((FieldAccessByIndexDescriptor) fd).reset(recType);
                                break;
                            }
                        }
                        throw new NotImplementedException("field-access-by-index for data of type " + t);
                    }
                    default: {
                        throw new NotImplementedException("field-access-by-index for data of type " + t);
                    }
                }
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.FIELD_ACCESS_NESTED, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
                AOrderedList fieldPath = (AOrderedList) (((AsterixConstantValue) ((ConstantExpression) fce
                        .getArguments().get(1).getValue()).getValue()).getObject());
                List<String> listFieldPath = new ArrayList<String>();
                for (int i = 0; i < fieldPath.size(); i++) {
                    listFieldPath.add(((AString) fieldPath.getItem(i)).getStringValue());
                }

                switch (t.getTypeTag()) {
                    case RECORD: {
                        ARecordType recType = (ARecordType) t;
                        ((FieldAccessNestedDescriptor) fd).reset(recType, listFieldPath);
                        break;
                    }
                    default: {
                        throw new NotImplementedException("field-access-nested for data of type " + t);
                    }
                }
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.GET_RECORD_FIELDS, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
                if (t.getTypeTag().equals(ATypeTag.RECORD)) {
                    ARecordType recType = (ARecordType) t;
                    ((GetRecordFieldsDescriptor) fd).reset(recType);
                } else {
                    throw new NotImplementedException("get-record-fields for data of type " + t);
                }
            }
        });
        functionTypeInferers.put(AsterixBuiltinFunctions.GET_RECORD_FIELD_VALUE, new FunctionTypeInferer() {
            @Override
            public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context)
                    throws AlgebricksException {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
                if (t.getTypeTag().equals(ATypeTag.RECORD)) {
                    ARecordType recType = (ARecordType) t;
                    ((GetRecordFieldValueDescriptor) fd).reset(recType);
                } else {
                    throw new NotImplementedException("get-record-field-value for data of type " + t);
                }
            }
        });

    }

    @Override
    public IPrinterFactoryProvider getADMPrinterFactoryProvider() {
        return AqlADMPrinterFactoryProvider.INSTANCE;
    }

    @Override
    public IPrinterFactoryProvider getLosslessJSONPrinterFactoryProvider() {
        return AqlLosslessJSONPrinterFactoryProvider.INSTANCE;
    }

    @Override
    public IPrinterFactoryProvider getCleanJSONPrinterFactoryProvider() {
        return AqlCleanJSONPrinterFactoryProvider.INSTANCE;
    }

    @Override
    public IPrinterFactoryProvider getCSVPrinterFactoryProvider() {
        return AqlCSVPrinterFactoryProvider.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IScalarEvaluatorFactory getConstantEvalFactory(IAlgebricksConstantValue value) throws AlgebricksException {
        IAObject obj = null;
        if (value.isNull()) {
            obj = ANull.NULL;
        } else if (value.isTrue()) {
            obj = ABoolean.TRUE;
        } else if (value.isFalse()) {
            obj = ABoolean.FALSE;
        } else {
            AsterixConstantValue acv = (AsterixConstantValue) value;
            obj = acv.getObject();
        }
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        DataOutput dos = abvs.getDataOutput();
        try {
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(obj.getType()).serialize(obj, dos);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        return new ConstantEvalFactory(Arrays.copyOf(abvs.getByteArray(), abvs.getLength()));
    }

    @Override
    public IBinaryIntegerInspectorFactory getBinaryIntegerInspectorFactory() {
        return AqlBinaryIntegerInspector.FACTORY;
    }

    @Override
    public INullWriterFactory getNullWriterFactory() {
        return AqlNullWriterFactory.INSTANCE;
    }

    @Override
    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer() {
        return new IExpressionEvalSizeComputer() {
            @Override
            public int getEvalSize(ILogicalExpression expr, IVariableEvalSizeEnvironment env)
                    throws AlgebricksException {
                switch (expr.getExpressionTag()) {
                    case CONSTANT: {
                        ConstantExpression c = (ConstantExpression) expr;
                        if (c == ConstantExpression.NULL) {
                            return 1;
                        } else if (c == ConstantExpression.FALSE || c == ConstantExpression.TRUE) {
                            return 2;
                        } else {
                            AsterixConstantValue acv = (AsterixConstantValue) c.getValue();
                            IAObject o = acv.getObject();
                            switch (o.getType().getTypeTag()) {
                                case DOUBLE: {
                                    return 9;
                                }
                                case BOOLEAN: {
                                    return 2;
                                }
                                case NULL: {
                                    return 1;
                                }
                                case INT32: {
                                    return 5;
                                }
                                case INT64: {
                                    return 9;
                                }
                                default: {
                                    // TODO
                                    return -1;
                                }
                            }
                        }
                    }
                    case FUNCTION_CALL: {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                        if (f.getFunctionIdentifier().equals(AsterixBuiltinFunctions.TID)) {
                            return 5;
                        } else {
                            // TODO
                            return -1;
                        }
                    }
                    default: {
                        // TODO
                        return -1;
                    }
                }
            }
        };
    }

    @Override
    public INormalizedKeyComputerFactoryProvider getNormalizedKeyComputerFactoryProvider() {
        return AqlNormalizedKeyComputerFactoryProvider.INSTANCE;
    }

    @Override
    public IBinaryHashFunctionFamilyProvider getBinaryHashFunctionFamilyProvider() {
        return AqlBinaryHashFunctionFamilyProvider.INSTANCE;
    }

    @Override
    public IPredicateEvaluatorFactoryProvider getPredicateEvaluatorFactoryProvider() {
        return AqlPredicateEvaluatorFactoryProvider.INSTANCE;
    }
}
