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
import java.util.List;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

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

            private DataOutput out = output.getDataOutput();
            private ByteArrayAccessibleOutputStream subRecordTmpStream = new ByteArrayAccessibleOutputStream();

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private ArrayBackedValueStorage[] abvsFields = new ArrayBackedValueStorage[fieldPath.size()];
            private DataOutput[] doFields = new DataOutput[fieldPath.size()];

            {
                FieldAccessUtil.getFieldsAbvs(abvsFields, doFields, fieldPath);
                recordType = recordType.deepCopy(recordType);
            }

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                FieldAccessUtil.evaluate(tuple, out, eval0, abvsFields, outInput0, subRecordTmpStream, recordType);
            }
        };
    }
}