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
package org.apache.asterix.app.function;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACollection;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.util.ResourceReleaseUtils;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.JSONUtil;

public class DumpIndexReader extends FunctionReader {

    private final CharArrayRecord record;
    private final IIndexCursor[] searchCursors;
    private final RecordDescriptor secondaryRecDesc;
    private final StringBuilder recordBuilder = new StringBuilder();
    private final ByteBufferInputStream bbis = new ByteBufferInputStream();
    private final DataInputStream dis = new DataInputStream(bbis);
    private final IIndexDataflowHelper[] indexDataflowHelpers;
    private final IIndexAccessor[] accessors;
    private int currentSearchIdx;

    public DumpIndexReader(IIndexDataflowHelper[] indexDataflowHelpers, RecordDescriptor secondaryRecDesc,
            IBinaryComparatorFactory[] comparatorFactories) throws HyracksDataException {
        this.indexDataflowHelpers = indexDataflowHelpers;
        this.secondaryRecDesc = secondaryRecDesc;
        MultiComparator searchMultiComparator = MultiComparator.create(comparatorFactories);
        RangePredicate rangePredicate =
                new RangePredicate(null, null, true, true, searchMultiComparator, searchMultiComparator, null, null);
        this.accessors = new IIndexAccessor[indexDataflowHelpers.length];
        this.searchCursors = new IIndexCursor[indexDataflowHelpers.length];
        for (int i = 0; i < indexDataflowHelpers.length; i++) {
            IIndexDataflowHelper indexDataflowHelper = indexDataflowHelpers[i];
            indexDataflowHelper.open();
            accessors[i] = indexDataflowHelper.getIndexInstance().createAccessor(NoOpIndexAccessParameters.INSTANCE);
            searchCursors[i] = accessors[i].createSearchCursor(false);
            accessors[i].search(searchCursors[i], rangePredicate);
        }
        currentSearchIdx = 0;
        record = new CharArrayRecord();
    }

    @Override
    public boolean hasNext() throws Exception {
        while (currentSearchIdx < searchCursors.length) {
            if (searchCursors[currentSearchIdx].hasNext()) {
                return true;
            }
            currentSearchIdx++;
        }
        return false;
    }

    @Override
    public IRawRecord<char[]> next() throws IOException, InterruptedException {
        searchCursors[currentSearchIdx].next();
        ITupleReference tuple = searchCursors[currentSearchIdx].getTuple();
        buildJsonRecord(tuple);
        record.reset();
        record.append(recordBuilder.toString().toCharArray());
        record.endRecord();
        return record;
    }

    @Override
    public void close() throws IOException {
        Throwable failure = releaseResources();
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    private Throwable releaseResources() {
        Throwable failure = CleanupUtils.close(bbis, null);
        failure = CleanupUtils.close(dis, failure);
        for (int i = 0; i < indexDataflowHelpers.length; i++) {
            failure = ResourceReleaseUtils.close(searchCursors[i], failure);
            failure = CleanupUtils.destroy(failure, searchCursors[i], accessors[i]);
            failure = ResourceReleaseUtils.close(indexDataflowHelpers[i], failure);
        }
        return failure;
    }

    private void buildJsonRecord(ITupleReference tuple) throws HyracksDataException {
        recordBuilder.setLength(0);
        recordBuilder.append("{\"values\":[");
        for (int j = 0; j < tuple.getFieldCount(); ++j) {
            bbis.setByteBuffer(ByteBuffer.wrap(tuple.getFieldData(j)), tuple.getFieldStart(j));
            IAObject field = (IAObject) secondaryRecDesc.getFields()[j].deserialize(dis);
            ATypeTag tag = field.getType().getTypeTag();
            if (tag == ATypeTag.MISSING) {
                continue;
            }
            printField(recordBuilder, field);
            recordBuilder.append(",");
        }
        recordBuilder.deleteCharAt(recordBuilder.length() - 1);
        recordBuilder.append("]}");
    }

    private void printField(StringBuilder sb, IAObject field) {
        ATypeTag typeTag = field.getType().getTypeTag();
        switch (typeTag) {
            case OBJECT:
                printObject(sb, ((ARecord) field));
                break;
            case ARRAY:
            case MULTISET:
                printCollection(sb, ((IACollection) field));
                break;
            case DATE:
            case TIME:
            case DATETIME:
                JSONUtil.quoteAndEscape(recordBuilder, field.toString());
                break;
            case STRING:
                JSONUtil.quoteAndEscape(recordBuilder, ((AString) field).getStringValue());
                break;
            case MISSING:
                break;
            default:
                sb.append(field);
        }
    }

    private void printObject(StringBuilder sb, ARecord record) {
        sb.append("{ ");
        int num = record.numberOfFields();
        ARecordType type = record.getType();
        for (int i = 0; i < num; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            IAObject value = record.getValueByPos(i);
            JSONUtil.quoteAndEscape(sb, type.getFieldNames()[i]);
            sb.append(": ");
            printField(sb, value);
        }
        sb.append(" }");
    }

    private void printCollection(StringBuilder sb, IACollection collection) {
        IACursor cursor = collection.getCursor();
        sb.append("[ ");
        boolean first = true;
        while (cursor.next()) {
            IAObject element = cursor.get();
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            printField(sb, element);
        }
        sb.append(" ]");
    }
}
