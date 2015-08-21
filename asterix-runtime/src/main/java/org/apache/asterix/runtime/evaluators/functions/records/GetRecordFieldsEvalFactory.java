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

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.pointables.nonvisitor.ARecordPointable;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetRecordFieldsEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ARecordType recordType;

    private final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();

    public GetRecordFieldsEvalFactory(ICopyEvaluatorFactory recordEvalFactory, ARecordType recordType) {
        this.recordEvalFactory = recordEvalFactory;
        this.recordType = recordType;
    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            private final ARecordPointable recordPointable = (ARecordPointable) ARecordPointable.FACTORY
                    .createPointable();

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private DataOutput out = output.getDataOutput();
            private RecordFieldsUtil rfu = new RecordFieldsUtil();

            {
                recordType = recordType.deepCopy(recordType);
            }

            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                outInput0.reset();
                eval0.evaluate(tuple);

                if (outInput0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                }

                if (outInput0.getByteArray()[0] != SER_RECORD_TYPE_TAG) {
                    throw new AlgebricksException("Field accessor is not defined for values of type "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0]));
                }

                recordPointable.set(outInput0.getByteArray(), outInput0.getStartOffset(), outInput0.getLength());

                try {
                    rfu.processRecord(recordPointable, recordType, out, 0);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (AsterixException e) {
                    e.printStackTrace();
                }
            }
        };
    }
}
