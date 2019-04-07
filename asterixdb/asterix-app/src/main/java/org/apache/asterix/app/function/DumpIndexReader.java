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
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;

public class DumpIndexReader extends FunctionReader {

    private final CharArrayRecord record;
    private final IIndexCursor searchCursor;
    private final RecordDescriptor secondaryRecDesc;
    private final StringBuilder recordBuilder = new StringBuilder();
    private final ByteBufferInputStream bbis = new ByteBufferInputStream();
    private final DataInputStream dis = new DataInputStream(bbis);
    private final IIndexDataflowHelper indexDataflowHelper;
    private final IIndexAccessor accessor;

    public DumpIndexReader(IIndexDataflowHelper indexDataflowHelper, RecordDescriptor secondaryRecDesc,
            IBinaryComparatorFactory[] comparatorFactories) throws HyracksDataException {
        this.indexDataflowHelper = indexDataflowHelper;
        this.secondaryRecDesc = secondaryRecDesc;
        indexDataflowHelper.open();
        IIndex indexInstance = indexDataflowHelper.getIndexInstance();
        accessor = indexInstance.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        searchCursor = accessor.createSearchCursor(false);
        MultiComparator searchMultiComparator = MultiComparator.create(comparatorFactories);
        RangePredicate rangePredicate =
                new RangePredicate(null, null, true, true, searchMultiComparator, searchMultiComparator, null, null);
        accessor.search(searchCursor, rangePredicate);
        record = new CharArrayRecord();
    }

    @Override
    public boolean hasNext() throws Exception {
        return searchCursor.hasNext();
    }

    @Override
    public IRawRecord<char[]> next() throws IOException, InterruptedException {
        searchCursor.next();
        ITupleReference tuple = searchCursor.getTuple();
        buildJsonRecord(tuple);
        record.reset();
        record.append(recordBuilder.toString().toCharArray());
        record.endRecord();
        return record;
    }

    @Override
    public void close() throws IOException {
        bbis.close();
        dis.close();
        if (searchCursor != null) {
            searchCursor.close();
            searchCursor.destroy();
        }
        if (accessor != null) {
            accessor.destroy();
        }
        indexDataflowHelper.close();
    }

    private void buildJsonRecord(ITupleReference tuple) throws HyracksDataException {
        recordBuilder.setLength(0);
        recordBuilder.append("{\"values\":[");
        for (int j = 0; j < tuple.getFieldCount(); ++j) {
            bbis.setByteBuffer(ByteBuffer.wrap(tuple.getFieldData(j)), tuple.getFieldStart(j));
            recordBuilder.append(secondaryRecDesc.getFields()[j].deserialize(dis));
            recordBuilder.append(",");
        }
        recordBuilder.deleteCharAt(recordBuilder.length() - 1);
        recordBuilder.append("]}");
    }
}
