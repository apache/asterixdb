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
package edu.uci.ics.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetRecordFieldValueEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ICopyEvaluatorFactory fldNameEvalFactory;
    private ARecordType recordType;

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

            private DataOutput out = output.getDataOutput();
            private ByteArrayAccessibleOutputStream subRecordTmpStream = new ByteArrayAccessibleOutputStream();

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private ICopyEvaluator eval1 = fldNameEvalFactory.createEvaluator(outInput1);

            int size = 1;
            private ArrayBackedValueStorage abvsFields[] = new ArrayBackedValueStorage[size];
            private DataOutput[] doFields = new DataOutput[size];

            @SuppressWarnings("unchecked")
            private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            {
                abvsFields[0] = new ArrayBackedValueStorage();
                doFields[0] = abvsFields[0].getDataOutput();
                recordType = recordType.deepCopy(recordType);
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

                    FieldAccessUtil.evaluate(tuple, out, eval0, abvsFields, outInput0, subRecordTmpStream, recordType);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }
}
