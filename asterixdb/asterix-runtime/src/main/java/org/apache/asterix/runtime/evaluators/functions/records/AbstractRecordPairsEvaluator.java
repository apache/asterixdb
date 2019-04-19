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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.CastTypeEvaluator;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

abstract class AbstractRecordPairsEvaluator implements IScalarEvaluator {
    protected final IScalarEvaluator eval0;
    protected final IPointable inputPointable = new VoidPointable();
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput resultOutput = resultStorage.getDataOutput();
    private CastTypeEvaluator inputCaster;

    AbstractRecordPairsEvaluator(IScalarEvaluator eval0, IAType inputType) {
        this.eval0 = eval0;
        if (inputType != null) {
            inputCaster = new CastTypeEvaluator(BuiltinType.ANY, inputType, eval0);
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        eval0.evaluate(tuple, inputPointable);

        if (PointableHelper.checkAndSetMissingOrNull(result, inputPointable)) {
            return;
        }

        final ATypeTag inputTypeTag = PointableHelper.getTypeTag(inputPointable);
        if (!validateInputType(inputTypeTag)) {
            PointableHelper.setNull(result);
            return;
        }
        inputCaster.evaluate(tuple, inputPointable);
        resultStorage.reset();
        buildOutput();
        result.set(resultStorage);
    }

    protected abstract boolean validateInputType(ATypeTag inputTypeTag);

    protected abstract void buildOutput() throws HyracksDataException;
}
