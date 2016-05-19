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
package org.apache.asterix.formats.base;

import java.util.List;

import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
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
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;

public interface IDataFormat {
    public void registerRuntimeFunctions(List<IFunctionDescriptorFactory> funcDescriptor) throws AlgebricksException;

    public ISerializerDeserializerProvider getSerdeProvider();

    public IBinaryHashFunctionFactoryProvider getBinaryHashFunctionFactoryProvider();

    public IBinaryComparatorFactoryProvider getBinaryComparatorFactoryProvider();

    public ITypeTraitProvider getTypeTraitProvider();

    public IBinaryBooleanInspectorFactory getBinaryBooleanInspectorFactory();

    public IBinaryIntegerInspectorFactory getBinaryIntegerInspectorFactory();

    // QQQ Refactor: Make this accept an APIFramework.OutputFormat parameter
    public IPrinterFactoryProvider getADMPrinterFactoryProvider();

    public IPrinterFactoryProvider getLosslessJSONPrinterFactoryProvider();

    public IPrinterFactoryProvider getCSVPrinterFactoryProvider();

    public IPrinterFactoryProvider getCleanJSONPrinterFactoryProvider();

    public INullWriterFactory getNullWriterFactory();

    public Triple<IScalarEvaluatorFactory, ScalarFunctionCallExpression, IAType> partitioningEvaluatorFactory(
            ARecordType recType, List<String> fldName) throws AlgebricksException;

    public IScalarEvaluatorFactory getFieldAccessEvaluatorFactory(ARecordType recType, List<String> fldName,
            int recordColumn) throws AlgebricksException;

    public IFunctionDescriptor resolveFunction(ILogicalExpression expr, IVariableTypeEnvironment typeEnvironment)
            throws AlgebricksException;

    public IScalarEvaluatorFactory getConstantEvalFactory(IAlgebricksConstantValue value) throws AlgebricksException;

    public IScalarEvaluatorFactory[] createMBRFactory(ARecordType recType, List<String> fldName, int recordColumn,
            int dimension, List<String> filterFieldName) throws AlgebricksException;

    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer();

    public INormalizedKeyComputerFactoryProvider getNormalizedKeyComputerFactoryProvider();

    public IBinaryHashFunctionFamilyProvider getBinaryHashFunctionFamilyProvider();

    public IPredicateEvaluatorFactoryProvider getPredicateEvaluatorFactoryProvider();
}
