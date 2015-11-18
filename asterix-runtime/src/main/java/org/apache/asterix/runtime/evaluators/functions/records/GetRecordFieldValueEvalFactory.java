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

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetRecordFieldValueEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ICopyEvaluatorFactory fldNameEvalFactory;
    private final ARecordType recordType;

    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    public GetRecordFieldValueEvalFactory(ICopyEvaluatorFactory recordEvalFactory,
            ICopyEvaluatorFactory fldNameEvalFactory, ARecordType recordType) {
        this.recordEvalFactory = recordEvalFactory;
        this.fldNameEvalFactory = fldNameEvalFactory;
        this.recordType = recordType;
    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {

            private final DataOutput out = output.getDataOutput();
            private final ByteArrayAccessibleOutputStream subRecordTmpStream = new ByteArrayAccessibleOutputStream();

            private final ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private final ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
            private final ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private final ICopyEvaluator eval1 = fldNameEvalFactory.createEvaluator(outInput1);

            private final int size = 1;
            private final ArrayBackedValueStorage abvsFields[] = new ArrayBackedValueStorage[size];
            private final DataOutput[] doFields = new DataOutput[size];

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);
            private final RuntimeRecordTypeInfo[] recTypeInfos = new RuntimeRecordTypeInfo[size];

            {
                abvsFields[0] = new ArrayBackedValueStorage();
                doFields[0] = abvsFields[0].getDataOutput();
                for (int index = 0; index < size; ++index) {
                    recTypeInfos[index] = new RuntimeRecordTypeInfo();
                }
            }

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                try {
                    outInput1.reset();
                    eval1.evaluate(tuple);

                    byte[] serFldName = outInput1.getByteArray();
                    if (serFldName[0] != SER_STRING_TYPE_TAG) {
                        nullSerde.serialize(ANull.NULL, out);
                        return;
                    }
                    abvsFields[0].reset();
                    doFields[0].write(serFldName);

                    FieldAccessUtil.evaluate(tuple, out, eval0, abvsFields, outInput0, subRecordTmpStream, recordType,
                            recTypeInfos);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }
}
