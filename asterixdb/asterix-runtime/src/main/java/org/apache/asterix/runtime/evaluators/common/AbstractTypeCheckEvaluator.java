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

package org.apache.asterix.runtime.evaluators.common;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractTypeCheckEvaluator implements IScalarEvaluator {

    protected enum Value {
        TRUE,
        FALSE,
        MISSING
    }

    protected static final byte[] MISSING_BYTES = new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG };
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput out = resultStorage.getDataOutput();
    protected final IPointable argPtr = new VoidPointable();
    protected final IScalarEvaluator eval;
    protected static final AObjectSerializerDeserializer aObjSerDer = AObjectSerializerDeserializer.INSTANCE;

    public AbstractTypeCheckEvaluator(IScalarEvaluator argEval) {
        this.eval = argEval;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        eval.evaluate(tuple, argPtr);
        Value match = isMatch(argPtr.getByteArray()[argPtr.getStartOffset()]);
        if (match == Value.MISSING) {
            result.set(MISSING_BYTES, 0, MISSING_BYTES.length);
            return;
        }
        ABoolean res = match == Value.TRUE ? ABoolean.TRUE : ABoolean.FALSE;
        resultStorage.reset();
        aObjSerDer.serialize(res, out);
        result.set(resultStorage);
    }

    protected abstract Value isMatch(byte typeTag);

}
