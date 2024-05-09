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

import static org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType.QUERY;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterEvaluatorFactory;
import org.apache.asterix.column.tuple.QueryColumnWithMetaTupleReference;
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

public class QueryColumnWithMetaTupleProjector extends QueryColumnTupleProjector {
    private final ARecordType metaType;
    private final ARecordType requestedMetaType;

    public QueryColumnWithMetaTupleProjector(ARecordType datasetType, ARecordType metaType, int numberOfPrimaryKeys,
            ARecordType requestedType, Map<String, FunctionCallInformation> functionCallInfoMap,
            ARecordType requestedMetaType, IColumnRangeFilterEvaluatorFactory filterEvaluator,
            IColumnIterableFilterEvaluatorFactory columnFilterEvaluatorFactory, IWarningCollector warningCollector,
            IHyracksTaskContext context) {
        super(datasetType, numberOfPrimaryKeys, requestedType, functionCallInfoMap, filterEvaluator,
                columnFilterEvaluatorFactory, warningCollector, context, QUERY);
        this.metaType = metaType;
        this.requestedMetaType = requestedMetaType;
    }

    @Override
    public IColumnProjectionInfo createProjectionInfo(IValueReference serializedMetadata) throws HyracksDataException {
        try {
            return QueryColumnWithMetaMetadata.create(datasetType, metaType, numberOfPrimaryKeys, serializedMetadata,
                    new ColumnValueReaderFactory(), ValueGetterFactory.INSTANCE, requestedType, functionCallInfoMap,
                    requestedMetaType, normalizedFilterEvaluatorFactory, columnFilterEvaluatorFactory, warningCollector,
                    context, projectorType);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    protected boolean isColumnar(ITupleReference tuple) {
        return tuple instanceof QueryColumnWithMetaTupleReference;
    }

    @Override
    protected IValueReference getAssembledValue(ITupleReference tuple) throws HyracksDataException {
        QueryColumnWithMetaTupleReference columnTuple = (QueryColumnWithMetaTupleReference) tuple;
        return columnTuple.getAssembledValue();
    }

    @Override
    protected void writeMeta(ITupleReference tuple, DataOutput dos, ArrayTupleBuilder tb) throws IOException {
        if (tuple instanceof QueryColumnWithMetaTupleReference) {
            QueryColumnWithMetaTupleReference columnTuple = (QueryColumnWithMetaTupleReference) tuple;
            IValueReference assembledRecord = columnTuple.getMetaAssembledValue();
            dos.write(assembledRecord.getByteArray(), assembledRecord.getStartOffset(), assembledRecord.getLength());
        } else {
            dos.write(tuple.getFieldData(numberOfPrimaryKeys + 1), tuple.getFieldStart(numberOfPrimaryKeys + 1),
                    tuple.getFieldLength(numberOfPrimaryKeys + 1));
        }
        tb.addFieldEndOffset();
    }

    @Override
    protected int getNumberOfTupleFields() {
        return super.getNumberOfTupleFields() + 1;
    }
}
