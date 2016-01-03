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
package org.apache.asterix.external.indexing;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.Serializable;
import java.util.Date;

import org.apache.asterix.external.operators.ExternalLookupOperatorDescriptor;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelper;
import org.apache.hyracks.storage.am.lsm.btree.impls.ExternalBTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;

/*
 * This class was created specifically to facilitate accessing 
 * external file index when doing external lookup during runtime
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ExternalFileIndexAccessor implements Serializable {

    private final FilesIndexDescription filesIndexDescription = new FilesIndexDescription();
    private static final long serialVersionUID = 1L;
    private ExternalBTreeDataflowHelper indexDataflowHelper;
    private ExternalLookupOperatorDescriptor opDesc;

    private IHyracksTaskContext ctx;
    private ExternalBTree index;
    private ArrayTupleBuilder searchKeyTupleBuilder;
    private ArrayTupleReference searchKey;
    private MultiComparator searchCmp;
    private AMutableInt32 currentFileNumber = new AMutableInt32(-1);
    private ISerializerDeserializer intSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
    private RangePredicate searchPredicate;
    private ILSMIndexAccessorInternal fileIndexAccessor;
    private IIndexCursor fileIndexSearchCursor;

    public ExternalFileIndexAccessor(ExternalBTreeDataflowHelper indexDataflowHelper,
            ExternalLookupOperatorDescriptor opDesc) {
        this.indexDataflowHelper = indexDataflowHelper;
        this.opDesc = opDesc;
    }

    public void open() throws HyracksDataException {
        // Open the index and get the instance
        indexDataflowHelper.open();
        index = (ExternalBTree) indexDataflowHelper.getIndexInstance();
        // Create search key and search predicate objects
        searchKey = new ArrayTupleReference();
        searchKeyTupleBuilder = new ArrayTupleBuilder(FilesIndexDescription.FILE_KEY_SIZE);
        searchKeyTupleBuilder.reset();
        searchKeyTupleBuilder.addField(intSerde, currentFileNumber);
        searchKey.reset(searchKeyTupleBuilder.getFieldEndOffsets(), searchKeyTupleBuilder.getByteArray());
        searchCmp = BTreeUtils.getSearchMultiComparator(index.getComparatorFactories(), searchKey);
        searchPredicate = new RangePredicate(searchKey, searchKey, true, true, searchCmp, searchCmp);

        // create the accessor  and the cursor using the passed version
        ISearchOperationCallback searchCallback = opDesc.getSearchOpCallbackFactory()
                .createSearchOperationCallback(indexDataflowHelper.getResourceID(), ctx);
        fileIndexAccessor = index.createAccessor(searchCallback, indexDataflowHelper.getVersion());
        fileIndexSearchCursor = fileIndexAccessor.createSearchCursor(false);
    }

    public void lookup(int fileId, ExternalFile file) throws Exception {
        // Set search parameters
        currentFileNumber.setValue(fileId);
        searchKeyTupleBuilder.reset();
        searchKeyTupleBuilder.addField(intSerde, currentFileNumber);
        searchKey.reset(searchKeyTupleBuilder.getFieldEndOffsets(), searchKeyTupleBuilder.getByteArray());
        fileIndexSearchCursor.reset();

        // Perform search
        fileIndexAccessor.search(fileIndexSearchCursor, searchPredicate);
        if (fileIndexSearchCursor.hasNext()) {
            fileIndexSearchCursor.next();
            ITupleReference tuple = fileIndexSearchCursor.getTuple();
            // Deserialize
            byte[] serRecord = tuple.getFieldData(FilesIndexDescription.FILE_PAYLOAD_INDEX);
            int recordStartOffset = tuple.getFieldStart(FilesIndexDescription.FILE_PAYLOAD_INDEX);
            int recordLength = tuple.getFieldLength(FilesIndexDescription.FILE_PAYLOAD_INDEX);
            ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
            DataInput in = new DataInputStream(stream);
            ARecord externalFileRecord = (ARecord) filesIndexDescription.EXTERNAL_FILE_RECORD_SERDE.deserialize(in);
            setFile(externalFileRecord, file);
        } else {
            // This should never happen
            throw new HyracksDataException("Was not able to find a file in the files index");
        }
    }

    private void setFile(ARecord externalFileRecord, ExternalFile file) {
        file.setFileName(
                ((AString) externalFileRecord.getValueByPos(FilesIndexDescription.EXTERNAL_FILE_NAME_FIELD_INDEX))
                        .getStringValue());
        file.setSize(((AInt64) externalFileRecord.getValueByPos(FilesIndexDescription.EXTERNAL_FILE_SIZE_FIELD_INDEX))
                .getLongValue());
        file.setLastModefiedTime((new Date(
                ((ADateTime) externalFileRecord.getValueByPos(FilesIndexDescription.EXTERNAL_FILE_MOD_DATE_FIELD_INDEX))
                        .getChrononTime())));
    }

    public void close() throws HyracksDataException {
        if (index != null) {
            try {
                fileIndexSearchCursor.close();
            } finally {
                indexDataflowHelper.close();
            }
        }
    }

}
