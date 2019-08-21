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
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.dataflow.data.nontagged.MissingWriterFactory;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.formats.nontagged.ADMPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFamilyProvider;
import org.apache.asterix.formats.nontagged.BinaryIntegerInspector;
import org.apache.asterix.formats.nontagged.CSVPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.CleanJSONPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.LosslessJSONPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.NormalizedKeyComputerFactoryProvider;
import org.apache.asterix.formats.nontagged.PredicateEvaluatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.common.CreateMBREvalFactory;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableEvalSizeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
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
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class NonTaggedDataFormat implements IDataFormat {

    static final NonTaggedDataFormat INSTANCE = new NonTaggedDataFormat();

    private static LogicalVariable METADATA_DUMMY_VAR = new LogicalVariable(-1);

    public static final String NON_TAGGED_DATA_FORMAT = "org.apache.asterix.runtime.formats.NonTaggedDataFormat";

    private NonTaggedDataFormat() {
    }

    @Override
    public IBinaryBooleanInspectorFactory getBinaryBooleanInspectorFactory() {
        return BinaryBooleanInspector.FACTORY;
    }

    @Override
    public IBinaryComparatorFactoryProvider getBinaryComparatorFactoryProvider() {
        return BinaryComparatorFactoryProvider.INSTANCE;
    }

    @Override
    public IBinaryHashFunctionFactoryProvider getBinaryHashFunctionFactoryProvider() {
        return BinaryHashFunctionFactoryProvider.INSTANCE;
    }

    @Override
    public ISerializerDeserializerProvider getSerdeProvider() {
        return SerializerDeserializerProvider.INSTANCE; // done
    }

    @Override
    public ITypeTraitProvider getTypeTraitProvider() {
        return TypeTraitProvider.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IScalarEvaluatorFactory getFieldAccessEvaluatorFactory(IFunctionManager functionManager, ARecordType recType,
            List<String> fldName, int recordColumn, SourceLocation sourceLoc) throws AlgebricksException {
        IScalarEvaluatorFactory recordEvalFactory = new ColumnAccessEvalFactory(recordColumn);

        if (fldName.size() == 1) {
            String[] names = recType.getFieldNames();
            ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            DataOutput dos = abvs.getDataOutput();

            String fieldName = fldName.get(0);
            for (int i = 0; i < names.length; i++) {
                if (names[i].equals(fieldName)) {
                    try {
                        AInt32 ai = new AInt32(i);
                        SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai,
                                dos);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    IScalarEvaluatorFactory fldIndexEvalFactory =
                            new ConstantEvalFactory(Arrays.copyOf(abvs.getByteArray(), abvs.getLength()));
                    IFunctionDescriptor fDesc =
                            functionManager.lookupFunction(BuiltinFunctions.FIELD_ACCESS_BY_INDEX, sourceLoc);
                    fDesc.setSourceLocation(sourceLoc);
                    fDesc.setImmutableStates(recType);
                    return fDesc.createEvaluatorFactory(
                            new IScalarEvaluatorFactory[] { recordEvalFactory, fldIndexEvalFactory });
                }
            }

            if (recType.isOpen()) {
                AString as = new AString(fieldName);
                try {
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as, dos);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
                IScalarEvaluatorFactory fldNameEvalFactory =
                        new ConstantEvalFactory(Arrays.copyOf(abvs.getByteArray(), abvs.getLength()));
                IFunctionDescriptor fDesc =
                        functionManager.lookupFunction(BuiltinFunctions.FIELD_ACCESS_BY_NAME, sourceLoc);
                fDesc.setSourceLocation(sourceLoc);
                return fDesc.createEvaluatorFactory(
                        new IScalarEvaluatorFactory[] { recordEvalFactory, fldNameEvalFactory });
            }
        }

        if (fldName.size() > 1) {
            IFunctionDescriptor fDesc = functionManager.lookupFunction(BuiltinFunctions.FIELD_ACCESS_NESTED, sourceLoc);
            fDesc.setSourceLocation(sourceLoc);
            fDesc.setImmutableStates(recType, fldName);
            return fDesc.createEvaluatorFactory(new IScalarEvaluatorFactory[] { recordEvalFactory });
        }

        throw new AlgebricksException("Could not find field " + fldName + " in the schema.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public IScalarEvaluatorFactory[] createMBRFactory(IFunctionManager functionManager, ARecordType recType,
            List<String> fldName, int recordColumn, int dimension, List<String> filterFieldName, boolean isPointMBR,
            SourceLocation sourceLoc) throws AlgebricksException {
        IScalarEvaluatorFactory evalFactory =
                getFieldAccessEvaluatorFactory(functionManager, recType, fldName, recordColumn, sourceLoc);
        int numOfFields = isPointMBR ? dimension : dimension * 2;
        IScalarEvaluatorFactory[] evalFactories =
                new IScalarEvaluatorFactory[numOfFields + (filterFieldName == null ? 0 : 1)];

        ArrayBackedValueStorage abvs1 = new ArrayBackedValueStorage();
        DataOutput dos1 = abvs1.getDataOutput();
        try {
            AInt32 ai = new AInt32(dimension);
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai, dos1);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        IScalarEvaluatorFactory dimensionEvalFactory =
                new ConstantEvalFactory(Arrays.copyOf(abvs1.getByteArray(), abvs1.getLength()));

        for (int i = 0; i < numOfFields; i++) {
            ArrayBackedValueStorage abvs2 = new ArrayBackedValueStorage();
            DataOutput dos2 = abvs2.getDataOutput();
            try {
                AInt32 ai = new AInt32(i);
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai, dos2);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            IScalarEvaluatorFactory coordinateEvalFactory =
                    new ConstantEvalFactory(Arrays.copyOf(abvs2.getByteArray(), abvs2.getLength()));

            evalFactories[i] = new CreateMBREvalFactory(evalFactory, dimensionEvalFactory, coordinateEvalFactory);
        }
        if (filterFieldName != null) {
            evalFactories[numOfFields] =
                    getFieldAccessEvaluatorFactory(functionManager, recType, filterFieldName, recordColumn, sourceLoc);
        }
        return evalFactories;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Triple<IScalarEvaluatorFactory, ScalarFunctionCallExpression, IAType> partitioningEvaluatorFactory(
            IFunctionManager functionManager, ARecordType recType, List<String> fldName, SourceLocation sourceLoc)
            throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        if (fldName.size() > 1) {
            for (int i = 0; i < n; i++) {
                if (names[i].equals(fldName.get(0))) {
                    IScalarEvaluatorFactory recordEvalFactory =
                            new ColumnAccessEvalFactory(GlobalConfig.DEFAULT_INPUT_DATA_COLUMN);
                    ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
                    DataOutput dos = abvs.getDataOutput();
                    try {
                        AInt32 ai = new AInt32(i);
                        SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(ai.getType()).serialize(ai,
                                dos);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                    IScalarEvaluatorFactory fldIndexEvalFactory =
                            new ConstantEvalFactory(Arrays.copyOf(abvs.getByteArray(), abvs.getLength()));
                    IFunctionDescriptor fDesc =
                            functionManager.lookupFunction(BuiltinFunctions.FIELD_ACCESS_BY_INDEX, sourceLoc);
                    fDesc.setSourceLocation(sourceLoc);
                    fDesc.setImmutableStates(recType);
                    IScalarEvaluatorFactory evalFactory = fDesc.createEvaluatorFactory(
                            new IScalarEvaluatorFactory[] { recordEvalFactory, fldIndexEvalFactory });
                    IFunctionInfo finfoAccess =
                            BuiltinFunctions.getAsterixFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_INDEX);

                    ScalarFunctionCallExpression partitionFun = new ScalarFunctionCallExpression(finfoAccess,
                            new MutableObject<>(new VariableReferenceExpression(METADATA_DUMMY_VAR)),
                            new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
                    partitionFun.setSourceLocation(sourceLoc);
                    return new Triple<>(evalFactory, partitionFun, recType.getFieldTypes()[i]);
                }
            }
        } else {
            IScalarEvaluatorFactory recordEvalFactory =
                    new ColumnAccessEvalFactory(GlobalConfig.DEFAULT_INPUT_DATA_COLUMN);
            ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            DataOutput dos = abvs.getDataOutput();
            AOrderedList as = new AOrderedList(fldName);
            try {
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(as.getType()).serialize(as, dos);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            IFunctionDescriptor fDesc = functionManager.lookupFunction(BuiltinFunctions.FIELD_ACCESS_NESTED, sourceLoc);
            fDesc.setSourceLocation(sourceLoc);
            fDesc.setImmutableStates(recType, fldName);
            IScalarEvaluatorFactory evalFactory =
                    fDesc.createEvaluatorFactory(new IScalarEvaluatorFactory[] { recordEvalFactory });
            IFunctionInfo finfoAccess = BuiltinFunctions.getAsterixFunctionInfo(BuiltinFunctions.FIELD_ACCESS_NESTED);

            ScalarFunctionCallExpression partitionFun = new ScalarFunctionCallExpression(finfoAccess,
                    new MutableObject<>(new VariableReferenceExpression(METADATA_DUMMY_VAR)),
                    new MutableObject<>(new ConstantExpression(new AsterixConstantValue(as))));
            partitionFun.setSourceLocation(sourceLoc);
            return new Triple<>(evalFactory, partitionFun, recType.getSubFieldType(fldName));
        }
        throw new AlgebricksException("Could not find field " + fldName + " in the schema.");
    }

    @Override
    public IPrinterFactoryProvider getADMPrinterFactoryProvider() {
        return ADMPrinterFactoryProvider.INSTANCE;
    }

    @Override
    public IPrinterFactoryProvider getLosslessJSONPrinterFactoryProvider() {
        return LosslessJSONPrinterFactoryProvider.INSTANCE;
    }

    @Override
    public IPrinterFactoryProvider getCleanJSONPrinterFactoryProvider() {
        return CleanJSONPrinterFactoryProvider.INSTANCE;
    }

    @Override
    public IPrinterFactoryProvider getCSVPrinterFactoryProvider() {
        return CSVPrinterFactoryProvider.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IScalarEvaluatorFactory getConstantEvalFactory(IAlgebricksConstantValue value) throws AlgebricksException {
        IAObject obj = null;
        if (value.isMissing()) {
            obj = AMissing.MISSING;
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
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(obj.getType()).serialize(obj, dos);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        return new ConstantEvalFactory(Arrays.copyOf(abvs.getByteArray(), abvs.getLength()));
    }

    @Override
    public IBinaryIntegerInspectorFactory getBinaryIntegerInspectorFactory() {
        return BinaryIntegerInspector.FACTORY;
    }

    @Override
    public IMissingWriterFactory getMissingWriterFactory() {
        return MissingWriterFactory.INSTANCE;
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
                        if (c == ConstantExpression.MISSING) {
                            return 1;
                        } else if (c == ConstantExpression.FALSE || c == ConstantExpression.TRUE) {
                            return 2;
                        } else {
                            AsterixConstantValue acv = (AsterixConstantValue) c.getValue();
                            IAObject o = acv.getObject();
                            switch (o.getType().getTypeTag()) {
                                case DOUBLE:
                                    return 9;
                                case FLOAT:
                                    return 5;
                                case BOOLEAN:
                                    return 2;
                                case MISSING:
                                    return 1;
                                case NULL:
                                    return 1;
                                case TINYINT:
                                    return 2;
                                case SMALLINT:
                                    return 3;
                                case INTEGER:
                                    return 5;
                                case BIGINT:
                                    return 9;
                                default:
                                    return -1;
                            }
                        }
                    }
                    case FUNCTION_CALL: {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                        if (f.getFunctionIdentifier().equals(BuiltinFunctions.TID)) {
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
        return NormalizedKeyComputerFactoryProvider.INSTANCE;
    }

    @Override
    public IBinaryHashFunctionFamilyProvider getBinaryHashFunctionFamilyProvider() {
        return BinaryHashFunctionFamilyProvider.INSTANCE;
    }

    @Override
    public IPredicateEvaluatorFactoryProvider getPredicateEvaluatorFactoryProvider() {
        return PredicateEvaluatorFactoryProvider.INSTANCE;
    }
}
