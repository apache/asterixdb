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

package org.apache.asterix.runtime.evaluators.functions.utils;

import java.io.DataOutput;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public final class RandomHelper {

    private final SecureRandom random;

    private double seed;

    private boolean isFirst;

    private final AMutableDouble aDouble = new AMutableDouble(0);

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput dataOutput = resultStorage.getDataOutput();

    public RandomHelper(boolean withSeed) throws HyracksDataException {
        if (withSeed) {
            try {
                random = SecureRandom.getInstance("SHA1PRNG");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "random()");
            }
        } else {
            random = new SecureRandom();
        }
        isFirst = true;
    }

    public void setSeed(double seedVal) throws HyracksDataException {
        if (isFirst || seedVal != seed) {
            seed = seedVal;
            isFirst = false;
            random.setSeed(Double.doubleToLongBits(seedVal));
        }
    }

    public void nextDouble(IPointable resultPointable) throws HyracksDataException {
        aDouble.setValue(random.nextDouble());
        resultStorage.reset();
        doubleSerde.serialize(aDouble, dataOutput);
        resultPointable.set(resultStorage);
    }
}
