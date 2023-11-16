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
package org.apache.asterix.column.operation.query;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.column.assembler.value.MissingValueGetter;
import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterEvaluatorFactory;
import org.apache.asterix.column.tuple.AssembledTupleReference;
import org.apache.asterix.column.tuple.QueryColumnTupleReference;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;

public class QueryColumnTupleProjector implements IColumnTupleProjector {
    protected final ARecordType datasetType;
    protected final ARecordType requestedType;
    protected final int numberOfPrimaryKeys;
    protected final Map<String, FunctionCallInformation> functionCallInfoMap;
    protected final IWarningCollector warningCollector;
    protected final IHyracksTaskContext context;
    protected final IColumnRangeFilterEvaluatorFactory normalizedFilterEvaluatorFactory;
    protected final IColumnIterableFilterEvaluatorFactory columnFilterEvaluatorFactory;
    private final AssembledTupleReference assembledTupleReference;

    public QueryColumnTupleProjector(ARecordType datasetType, int numberOfPrimaryKeys, ARecordType requestedType,
            Map<String, FunctionCallInformation> functionCallInfoMap,
            IColumnRangeFilterEvaluatorFactory normalizedFilterEvaluatorFactory,
            IColumnIterableFilterEvaluatorFactory columnFilterEvaluatorFactory, IWarningCollector warningCollector,
            IHyracksTaskContext context) {
        this.datasetType = datasetType;
        this.numberOfPrimaryKeys = numberOfPrimaryKeys;
        this.requestedType = requestedType;
        this.functionCallInfoMap = functionCallInfoMap;
        this.normalizedFilterEvaluatorFactory = normalizedFilterEvaluatorFactory;
        this.columnFilterEvaluatorFactory = columnFilterEvaluatorFactory;
        this.warningCollector = warningCollector;
        this.context = context;
        assembledTupleReference = new AssembledTupleReference(getNumberOfTupleFields());
    }

    @Override
    public IColumnProjectionInfo createProjectionInfo(IValueReference serializedMetadata) throws HyracksDataException {
        try {
            return QueryColumnMetadata.create(datasetType, numberOfPrimaryKeys, serializedMetadata,
                    new ColumnValueReaderFactory(), ValueGetterFactory.INSTANCE, requestedType, functionCallInfoMap,
                    normalizedFilterEvaluatorFactory, columnFilterEvaluatorFactory, warningCollector, context);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public final ITupleReference project(ITupleReference tuple, DataOutput dos, ArrayTupleBuilder tb)
            throws IOException {
        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            tb.addFieldEndOffset();
        }
        if (isColumnar(tuple)) {
            IValueReference assembledRecord = getAssembledValue(tuple);
            if (assembledRecord == MissingValueGetter.MISSING) {
                return null;
            }
            dos.write(assembledRecord.getByteArray(), assembledRecord.getStartOffset(), assembledRecord.getLength());
        } else {
            dos.write(tuple.getFieldData(numberOfPrimaryKeys), tuple.getFieldStart(numberOfPrimaryKeys),
                    tuple.getFieldLength(numberOfPrimaryKeys));
        }
        tb.addFieldEndOffset();
        //Write meta (if any)
        writeMeta(tuple, dos, tb);

        return assembledTupleReference.reset(tb);
    }

    protected boolean isColumnar(ITupleReference tuple) {
        return tuple instanceof QueryColumnTupleReference;
    }

    protected IValueReference getAssembledValue(ITupleReference tuple) throws HyracksDataException {
        QueryColumnTupleReference columnTuple = (QueryColumnTupleReference) tuple;
        return columnTuple.getAssembledValue();
    }

    protected void writeMeta(ITupleReference tuple, DataOutput dos, ArrayTupleBuilder tb) throws IOException {
        // NoOp
    }

    protected int getNumberOfTupleFields() {
        return numberOfPrimaryKeys + 1;
    }

}
