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
package edu.uci.ics.asterix.formats.base;

import edu.uci.ics.asterix.common.parse.IParseFileSplitsDecl;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public interface IDataFormat {
    public void registerRuntimeFunctions() throws AlgebricksException;

    public ISerializerDeserializerProvider getSerdeProvider();

    public IBinaryHashFunctionFactoryProvider getBinaryHashFunctionFactoryProvider();

    public IBinaryComparatorFactoryProvider getBinaryComparatorFactoryProvider();

    public ITypeTraitProvider getTypeTraitProvider();

    public IBinaryBooleanInspectorFactory getBinaryBooleanInspectorFactory();

    public IBinaryIntegerInspectorFactory getBinaryIntegerInspectorFactory();

    public IPrinterFactoryProvider getPrinterFactoryProvider();

    public IPrinterFactoryProvider getJSONPrinterFactoryProvider();

    public INullWriterFactory getNullWriterFactory();

    public Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType> partitioningEvaluatorFactory(
            ARecordType recType, String fldName) throws AlgebricksException;

    public ICopyEvaluatorFactory getFieldAccessEvaluatorFactory(ARecordType recType, String fldName, int recordColumn)
            throws AlgebricksException;

    public ITupleParserFactory createTupleParser(ARecordType recType, IParseFileSplitsDecl decl);

    public ITupleParserFactory createTupleParser(ARecordType recType, boolean isDelimited, Character delimiter);

    public IFunctionDescriptor resolveFunction(ILogicalExpression expr, IVariableTypeEnvironment typeEnvironment)
            throws AlgebricksException;

    public ICopyEvaluatorFactory getConstantEvalFactory(IAlgebricksConstantValue value) throws AlgebricksException;

    public ICopyEvaluatorFactory[] createMBRFactory(ARecordType recType, String fldName, int recordColumn, int dimension)
            throws AlgebricksException;

    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer();

    public INormalizedKeyComputerFactoryProvider getNormalizedKeyComputerFactoryProvider();

    public IBinaryHashFunctionFamilyProvider getBinaryHashFunctionFamilyProvider();
    
    public IPredicateEvaluatorFactoryProvider getPredicateEvaluatorFactoryProvider();
}
