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
package edu.uci.ics.asterix.dataflow.data.nontagged.hash;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.hierachy.FloatToDoubleTypeConvertComputer;
import edu.uci.ics.asterix.om.types.hierachy.IntegerToDoubleTypeConvertComputer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHash;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class AMurmurHash3BinaryHashFunctionFamily implements IBinaryHashFunctionFamily {

    public static final IBinaryHashFunctionFamily INSTANCE = new AMurmurHash3BinaryHashFunctionFamily();

    private static final long serialVersionUID = 1L;

    private AMurmurHash3BinaryHashFunctionFamily() {
    }

    // This hash function family is used to promote a numeric type to a DOUBLE numeric type
    // to return same hash value for the original numeric value, regardless of the numeric type.
    // (e.g., h( int64("1") )  =  h( double("1.0") )

    @Override
    public IBinaryHashFunction createBinaryHashFunction(final int seed) {
        return new IBinaryHashFunction() {

            private ArrayBackedValueStorage fieldValueBuffer = new ArrayBackedValueStorage();
            private DataOutput fieldValueBufferOutput = fieldValueBuffer.getDataOutput();
            private ATypeTag sourceTag = null;
            private boolean numericTypePromotionApplied = false;

            @Override
            public int hash(byte[] bytes, int offset, int length) throws HyracksDataException {

                // If a numeric type is encountered, then we promote each numeric type to the DOUBLE type.
                fieldValueBuffer.reset();
                sourceTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);

                switch (sourceTag) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                        try {
                            IntegerToDoubleTypeConvertComputer.INSTANCE.convertType(bytes, offset + 1, length - 1,
                                    fieldValueBufferOutput);
                        } catch (IOException e) {
                            throw new HyracksDataException(
                                    "A numeric type promotion error has occurred before doing hash(). Can't continue process. Detailed Error message:"
                                            + e.getMessage());
                        }
                        numericTypePromotionApplied = true;
                        break;

                    case FLOAT:
                        try {
                            FloatToDoubleTypeConvertComputer.INSTANCE.convertType(bytes, offset + 1, length - 1,
                                    fieldValueBufferOutput);
                        } catch (IOException e) {
                            throw new HyracksDataException(
                                    "A numeric type promotion error has occurred before doing hash(). Can't continue process. Detailed Error message:"
                                            + e.getMessage());
                        }
                        numericTypePromotionApplied = true;
                        break;

                    default:
                        numericTypePromotionApplied = false;
                        break;
                }

                // If a numeric type promotion happened
                if (numericTypePromotionApplied) {
                    return MurmurHash3BinaryHash.hash(fieldValueBuffer.getByteArray(),
                            fieldValueBuffer.getStartOffset(), fieldValueBuffer.getLength(), seed);

                } else {
                    // Usual case for non numeric types and the DOBULE numeric type
                    return MurmurHash3BinaryHash.hash(bytes, offset, length, seed);
                }
            }
        };
    }
}