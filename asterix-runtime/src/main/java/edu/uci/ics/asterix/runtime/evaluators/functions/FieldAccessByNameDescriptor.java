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
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FieldAccessByNameDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new FieldAccessByNameDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(ICopyEvaluatorFactory[] args) {
        return new FieldAccessByNameEvalFactory(args[0], args[1]);
    }

    private static class FieldAccessByNameEvalFactory implements ICopyEvaluatorFactory {

        private static final long serialVersionUID = 1L;

        private ICopyEvaluatorFactory recordEvalFactory;
        private ICopyEvaluatorFactory fldNameEvalFactory;

        private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
        private final static byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();

        public FieldAccessByNameEvalFactory(ICopyEvaluatorFactory recordEvalFactory,
                ICopyEvaluatorFactory fldNameEvalFactory) {
            this.recordEvalFactory = recordEvalFactory;
            this.fldNameEvalFactory = fldNameEvalFactory;
        }

        @Override
        public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
            return new ICopyEvaluator() {

                private DataOutput out = output.getDataOutput();

                private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
                private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
                private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
                private ICopyEvaluator eval1 = fldNameEvalFactory.createEvaluator(outInput1);
                @SuppressWarnings("unchecked")
                private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                        .getSerializerDeserializer(BuiltinType.ANULL);
                private int fieldValueOffset;
                private int fieldValueLength;
                private ATypeTag fieldValueTypeTag = ATypeTag.NULL;

                @Override
                public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                    try {
                        outInput0.reset();
                        eval0.evaluate(tuple);
                        outInput1.reset();
                        eval1.evaluate(tuple);
                        byte[] serRecord = outInput0.getByteArray();

                        if (serRecord[0] == SER_NULL_TYPE_TAG) {
                            nullSerde.serialize(ANull.NULL, out);
                            return;
                        }

                        if (serRecord[0] != SER_RECORD_TYPE_TAG) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME.getName()
                                    + ": expects input type NULL or RECORD, but got "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serRecord[0]));
                        }

                        byte[] serFldName = outInput1.getByteArray();
                        fieldValueOffset = ARecordSerializerDeserializer.getFieldOffsetByName(serRecord, serFldName);
                        if (fieldValueOffset < 0) {
                            out.writeByte(ATypeTag.NULL.serialize());
                            return;
                        }

                        fieldValueTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                                .deserialize(serRecord[fieldValueOffset]);
                        fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(serRecord, fieldValueOffset,
                                fieldValueTypeTag, true) + 1;
                        out.write(serRecord, fieldValueOffset, fieldValueLength);

                    } catch (IOException e) {
                        throw new AlgebricksException(e);
                    } catch (AsterixException e) {
                        throw new AlgebricksException(e);
                    }
                }
            };
        }

    }
}
