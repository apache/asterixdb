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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetRecordFieldsEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory recordEvalFactory;
    private final ARecordType recordType;

    public GetRecordFieldsEvalFactory(IScalarEvaluatorFactory recordEvalFactory, ARecordType recordType) {
        this.recordEvalFactory = recordEvalFactory;
        this.recordType = recordType;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
        return new IScalarEvaluator() {

            private final ARecordPointable recordPointable = (ARecordPointable) ARecordPointable.FACTORY
                    .createPointable();
            private IPointable inputArg0 = new VoidPointable();
            private IScalarEvaluator eval0 = recordEvalFactory.createScalarEvaluator(ctx);
            private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
            private DataOutput out = resultStorage.getDataOutput();
            private RecordFieldsUtil rfu = new RecordFieldsUtil();

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                resultStorage.reset();
                eval0.evaluate(tuple, inputArg0);
                byte[] data = inputArg0.getByteArray();
                int offset = inputArg0.getStartOffset();
                int len = inputArg0.getLength();

                if (data[offset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                    throw new TypeMismatchException(AsterixBuiltinFunctions.GET_RECORD_FIELDS, 0, data[offset],
                            ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
                }

                recordPointable.set(data, offset, len);
                try {
                    rfu.processRecord(recordPointable, recordType, out, 0);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                } catch (AsterixException e) {
                    throw new HyracksDataException(e);
                }
                result.set(resultStorage);
            }
        };
    }
}
