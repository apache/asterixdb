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

package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.cast.ACastVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.BinaryHashMap;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.BinaryEntry;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

class RecordConcatEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory[] args;

    private final ARecordType[] argTypes;

    private final boolean failOnArgTypeMismatch;

    private final SourceLocation sourceLoc;

    RecordConcatEvalFactory(IScalarEvaluatorFactory[] args, ARecordType[] argTypes, boolean failOnArgTypeMismatch,
            SourceLocation sourceLoc) {
        this.args = args;
        this.argTypes = argTypes;
        this.failOnArgTypeMismatch = failOnArgTypeMismatch;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
        IScalarEvaluator[] argEvals = new IScalarEvaluator[args.length];
        for (int i = 0; i < args.length; i++) {
            argEvals[i] = args[i].createScalarEvaluator(ctx);
        }
        return new RecordConcatEvaluator(argEvals);
    }

    private final class RecordConcatEvaluator implements IScalarEvaluator {

        private static final int TABLE_FRAME_SIZE = 32768;
        private static final int TABLE_SIZE = 100;

        private final IScalarEvaluator[] argEvals;
        private final IPointable[] argPointables;
        private final ARecordVisitablePointable[] argRecordPointables;
        private final ARecordVisitablePointable openRecordPointable;

        private final BitSet castRequired;
        private ACastVisitor castVisitor;
        private Triple<IVisitablePointable, IAType, Boolean> castVisitorArg;

        private final RecordBuilder outRecordBuilder;
        private final ArrayBackedValueStorage resultStorage;
        private final DataOutput resultOutput;

        private final BinaryHashMap fieldMap;
        private final BinaryEntry keyEntry;
        private final BinaryEntry valEntry;

        private RecordConcatEvaluator(IScalarEvaluator[] argEvals) {
            this.argEvals = argEvals;

            argPointables = new IPointable[args.length];
            argRecordPointables = new ARecordVisitablePointable[args.length];
            openRecordPointable = new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);

            resultStorage = new ArrayBackedValueStorage();
            resultOutput = resultStorage.getDataOutput();
            outRecordBuilder = new RecordBuilder();
            outRecordBuilder.reset(openRecordPointable.getInputRecordType());

            fieldMap = new BinaryHashMap(TABLE_SIZE, TABLE_FRAME_SIZE, outRecordBuilder.getFieldNameHashFunction(),
                    outRecordBuilder.getFieldNameHashFunction(), outRecordBuilder.getFieldNameComparator());
            keyEntry = new BinaryEntry();
            valEntry = new BinaryEntry();
            valEntry.set(new byte[0], 0, 0);

            castRequired = new BitSet();
            for (int i = 0; i < args.length; i++) {
                argPointables[i] = new VoidPointable();
                ARecordType argType = argTypes[i];
                if (argType != null) {
                    argRecordPointables[i] = new ARecordVisitablePointable(argType);
                    if (hasDerivedType(argType.getFieldTypes())) {
                        castRequired.set(i);
                        if (castVisitor == null) {
                            castVisitor = new ACastVisitor();
                            castVisitorArg = new Triple<>(openRecordPointable, openRecordPointable.getInputRecordType(),
                                    Boolean.FALSE);
                        }
                    }
                }
            }
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            resultStorage.reset();
            if (validateArgs(tuple)) {
                processArgs();
            }
            result.set(resultStorage);
        }

        private boolean validateArgs(IFrameTupleReference tuple) throws HyracksDataException {
            if (args.length == 0) {
                writeTypeTag(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                return false;
            }
            boolean returnMissing = false, returnNull = false;
            for (int i = 0; i < argEvals.length; i++) {
                IPointable argPtr = argPointables[i];
                argEvals[i].evaluate(tuple, argPtr);

                byte[] data = argPtr.getByteArray();
                int offset = argPtr.getStartOffset();
                byte typeTag = data[offset];

                if (typeTag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                    returnMissing = true;
                    if (!failOnArgTypeMismatch) {
                        break;
                    }
                } else if (typeTag == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                    returnNull = true;
                } else if (typeTag != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                    if (failOnArgTypeMismatch) {
                        throw new TypeMismatchException(sourceLoc, BuiltinFunctions.RECORD_CONCAT, i, typeTag,
                                ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
                    } else {
                        returnNull = true;
                    }
                }
            }
            if (returnMissing) {
                writeTypeTag(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
                return false;
            }
            if (returnNull) {
                writeTypeTag(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                return false;
            }
            return true;
        }

        private void processArgs() throws HyracksDataException {
            outRecordBuilder.init();
            fieldMap.clear();
            for (int i = argEvals.length - 1; i >= 0; i--) {
                try {
                    appendRecord(argPointables[i], argRecordPointables[i], castRequired.get(i));
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
            outRecordBuilder.write(resultOutput, true);
        }

        private void appendRecord(IPointable recordPtr, ARecordVisitablePointable argVisitablePointable,
                boolean argCastRequired) throws IOException {

            ARecordVisitablePointable recordPointable;
            if (argVisitablePointable != null) {
                argVisitablePointable.set(recordPtr);
                if (argCastRequired) {
                    argVisitablePointable.accept(castVisitor, castVisitorArg);
                    recordPointable = openRecordPointable;
                } else {
                    recordPointable = argVisitablePointable;
                }
            } else {
                openRecordPointable.set(recordPtr);
                recordPointable = openRecordPointable;
            }

            List<IVisitablePointable> fieldNames = recordPointable.getFieldNames();
            List<IVisitablePointable> fieldValues = recordPointable.getFieldValues();
            for (int i = 0, fieldCount = fieldNames.size(); i < fieldCount; i++) {
                IVisitablePointable fieldName = fieldNames.get(i);
                if (canAppendField(fieldName.getByteArray(), fieldName.getStartOffset() + 1,
                        fieldName.getLength() - 1)) {
                    outRecordBuilder.addField(fieldName, fieldValues.get(i));
                }
            }
        }

        private boolean canAppendField(byte[] buf, int offset, int length) throws HyracksDataException {
            keyEntry.set(buf, offset, length);
            return fieldMap.put(keyEntry, valEntry) == null;
        }

        private boolean hasDerivedType(IAType[] types) {
            for (IAType type : types) {
                if (type.getTypeTag().isDerivedType()) {
                    return true;
                }
            }
            return false;
        }

        private void writeTypeTag(byte typeTag) throws HyracksDataException {
            try {
                resultOutput.writeByte(typeTag);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }
}
