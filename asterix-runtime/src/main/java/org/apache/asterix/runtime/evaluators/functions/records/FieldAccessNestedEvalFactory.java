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
import java.util.List;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FieldAccessNestedEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ARecordType recordType;
    private List<String> fieldPath;

    public FieldAccessNestedEvalFactory(ICopyEvaluatorFactory recordEvalFactory, ARecordType recordType,
            List<String> fldName) {
        this.recordEvalFactory = recordEvalFactory;
        this.recordType = recordType;
        this.fieldPath = fldName;

    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {

            private final DataOutput out = output.getDataOutput();
            private final ByteArrayAccessibleOutputStream subRecordTmpStream = new ByteArrayAccessibleOutputStream();

            private final ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private final ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private final ArrayBackedValueStorage[] abvsFields = new ArrayBackedValueStorage[fieldPath.size()];
            private final DataOutput[] doFields = new DataOutput[fieldPath.size()];
            private final RuntimeRecordTypeInfo[] recTypeInfos = new RuntimeRecordTypeInfo[fieldPath.size()];

            {
                FieldAccessUtil.getFieldsAbvs(abvsFields, doFields, fieldPath);
                for (int index = 0; index < fieldPath.size(); ++index) {
                    recTypeInfos[index] = new RuntimeRecordTypeInfo();
                }

            }

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                FieldAccessUtil.evaluate(tuple, out, eval0, abvsFields, outInput0, subRecordTmpStream, recordType,
                        recTypeInfos);
            }
        };
    }
}