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
package edu.uci.ics.asterix.runtime.evaluators.functions.temporal;

import java.io.DataOutput;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.temporal.AsterixTemporalTypeParseException;
import edu.uci.ics.asterix.om.base.temporal.DateTimeFormatUtils;
import edu.uci.ics.asterix.om.base.temporal.DateTimeFormatUtils.DateTimeParseMode;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ParseDateTimeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = AsterixBuiltinFunctions.PARSE_DATETIME;

    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static DateTimeFormatUtils DT_UTILS = DateTimeFormatUtils.getInstance();

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ParseDateTimeDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();
                    private ArrayBackedValueStorage argOut0 = new ArrayBackedValueStorage();
                    private ArrayBackedValueStorage argOut1 = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval0 = args[0].createEvaluator(argOut0);
                    private ICopyEvaluator eval1 = args[1].createEvaluator(argOut1);

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADateTime> datetimeSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADATETIME);

                    private AMutableDateTime aDateTime = new AMutableDateTime(0);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut0.reset();
                        eval0.evaluate(tuple);
                        argOut1.reset();
                        eval1.evaluate(tuple);

                        try {
                            if (argOut0.getByteArray()[0] == SER_NULL_TYPE_TAG
                                    || argOut1.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                return;
                            }

                            if (argOut0.getByteArray()[0] != SER_STRING_TYPE_TAG
                                    || argOut1.getByteArray()[0] != SER_STRING_TYPE_TAG) {
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": expects two strings but got  ("
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut0.getByteArray()[0])
                                        + ", "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut1.getByteArray()[0])
                                        + ")");
                            }
                            int length0 = (argOut0.getByteArray()[1] & 0xff << 8)
                                    + (argOut0.getByteArray()[2] & 0xff << 0);
                            int length1 = (argOut1.getByteArray()[1] & 0xff << 8)
                                    + (argOut1.getByteArray()[2] & 0xff << 0);
                            long chronon = 0;

                            int formatStart = 3;
                            int formatLength = 0;
                            boolean processSuccessfully = false;
                            while (!processSuccessfully && formatStart < 3 + length1) {
                                // search for "|"
                                formatLength = 0;
                                for (; formatStart + formatLength < 3 + length1; formatLength++) {
                                    if (argOut1.getByteArray()[formatStart + formatLength] == '|') {
                                        break;
                                    }
                                }
                                try {
                                    chronon = DT_UTILS.parseDateTime(argOut0.getByteArray(), 3, length0,
                                            argOut1.getByteArray(), formatStart, formatLength,
                                            DateTimeParseMode.DATETIME);
                                } catch (AsterixTemporalTypeParseException ex) {
                                    formatStart += formatLength + 1;
                                    continue;
                                }
                                processSuccessfully = true;
                            }

                            if (!processSuccessfully) {
                                throw new HyracksDataException(
                                        "parse-date: Failed to match with any given format string!");
                            }

                            aDateTime.setValue(chronon);
                            datetimeSerde.serialize(aDateTime, out);

                        } catch (HyracksDataException ex) {
                            throw new AlgebricksException(ex);
                        }
                    }
                };
            }

        };
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.functions.AbstractFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
