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

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ClosedRecordConstructorEvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory[] args;
    private ARecordType recType;

    public ClosedRecordConstructorEvalFactory(IScalarEvaluatorFactory[] args, ARecordType recType) {
        this.args = args;
        this.recType = recType;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
        int n = args.length / 2;
        IScalarEvaluator[] evalFields = new IScalarEvaluator[n];
        for (int i = 0; i < n; i++) {
            evalFields[i] = args[2 * i + 1].createScalarEvaluator(ctx);
        }
        return new ClosedRecordConstructorEval(recType, evalFields);
    }

    public static class ClosedRecordConstructorEval implements IScalarEvaluator {
        private IScalarEvaluator[] evalFields;
        private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
        private final DataOutput out = resultStorage.getDataOutput();
        private final IARecordBuilder recBuilder = new RecordBuilder();
        private final ARecordType recType;
        private final IPointable fieldValuePointable = new VoidPointable();
        private boolean first = true;

        public ClosedRecordConstructorEval(ARecordType recType, IScalarEvaluator[] evalFields) {
            this.evalFields = evalFields;
            this.recType = recType;
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            resultStorage.reset();
            if (first) {
                first = false;
                recBuilder.reset(this.recType);
            }
            recBuilder.init();
            for (int i = 0; i < evalFields.length; i++) {
                evalFields[i].evaluate(tuple, fieldValuePointable);
                recBuilder.addField(i, fieldValuePointable);
            }
            recBuilder.write(out, true);
            result.set(resultStorage);
        }
    }

}
