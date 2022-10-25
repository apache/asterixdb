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

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * Base evaluator class for the following functions:
 * 1. {@link org.apache.asterix.om.functions.BuiltinFunctions#RECORD_ADD}
 * 2. {@link org.apache.asterix.om.functions.BuiltinFunctions#RECORD_PUT}
 * 3. {@link org.apache.asterix.om.functions.BuiltinFunctions#RECORD_REMOVE}
 * 4. {@link org.apache.asterix.om.functions.BuiltinFunctions#RECORD_RENAME}
 */
abstract class AbstractRecordFunctionEvaluator implements IScalarEvaluator {
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput resultOutput = resultStorage.getDataOutput();
    protected final RecordBuilder outRecordBuilder = new RecordBuilder();

    protected final IPointable newFieldNamePointable = new VoidPointable();
    protected final IPointable newFieldValuePointable = new VoidPointable();
    protected final IBinaryComparator stringBinaryComparator =
            UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

    protected final PointableAllocator pointableAllocator = new PointableAllocator();
    protected final IPointable inputPointable = new VoidPointable();
    protected final ARecordType inRecType;
    protected ARecordVisitablePointable inputRecordPointable;

    protected final ARecordType outRecType;
    protected final RuntimeRecordTypeInfo outputRecordTypeInfo = new RuntimeRecordTypeInfo();

    AbstractRecordFunctionEvaluator(ARecordType outRecType, ARecordType inRecType) {
        this.outRecType = outRecType;
        this.inRecType = inRecType;
    }
}
