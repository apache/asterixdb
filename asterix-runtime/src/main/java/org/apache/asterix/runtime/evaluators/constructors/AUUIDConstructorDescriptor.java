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
package org.apache.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.UnsupportedEncodingException;

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AUUID;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AUUIDConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AUUIDConstructorDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<AUUID> uuidSerDe = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.AUUID);

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {

                return new ICopyEvaluator() {
                    private DataOutput out = output.getDataOutput();
                    private ArrayBackedValueStorage outInput = new ArrayBackedValueStorage();
                    private ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
                    private ICopyEvaluator eval = args[0].createEvaluator(outInput);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AUUID> uuidSerDe = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AUUID);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        outInput.reset();
                        baaos.reset();
                        eval.evaluate(tuple);
                        byte[] serString = outInput.getByteArray();

                        AUUID uuid;
                        try {
                            uuid = AUUID.fromString(new String(serString, 3, outInput.getLength() - 3, "UTF-8"));

                            ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[serString[0]];
                            if (tt == ATypeTag.NULL) {
                                nullSerde.serialize(ANull.NULL, out);
                            } else if (tt == ATypeTag.STRING) {
                                uuidSerDe.serialize(uuid, output.getDataOutput());
                            } else {
                                throw new AlgebricksException("uuid of " + tt + " not supported");
                            }
                        } catch (UnsupportedEncodingException e) {
                            throw new AlgebricksException(e);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }

                    }

                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.UUID_CONSTRUCTOR;
    }

}