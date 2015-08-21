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
package edu.uci.ics.asterix.metadata.external;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.Serializable;
import java.util.Date;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.entities.ExternalFile;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelper;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.ExternalBTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;

/*
 * This class was created specifically to facilitate accessing 
 * external file index when doing external lookup during runtime
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ExternalFileIndexAccessor implements Serializable {

    private static final long serialVersionUID = 1L;
    private ExternalBTreeDataflowHelper indexDataflowHelper;
    private ExternalLoopkupOperatorDiscriptor opDesc;

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

    public ExternalFileIndexAccessor(ExternalBTreeDataflowHelper indexDataflowHelper, ExternalLoopkupOperatorDiscriptor opDesc) {
        this.indexDataflowHelper = indexDataflowHelper;
        this.opDesc = opDesc;
    }

    public void openIndex() throws HyracksDataException {
        // Open the index and get the instance
        indexDataflowHelper.open();
        index = (ExternalBTree) indexDataflowHelper.getIndexInstance();
        try {
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
        } catch (Exception e) {
            indexDataflowHelper.close();
            throw new HyracksDataException(e);
        }
    }

    public void searchForFile(int fileNumber, ExternalFile file) throws Exception {
        // Set search parameters
        currentFileNumber.setValue(fileNumber);
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
            ARecord externalFileRecord = (ARecord) FilesIndexDescription.EXTERNAL_FILE_RECORD_SERDE.deserialize(in);
            setExternalFileFromARecord(externalFileRecord, file);
        } else {
            // This should never happen
            throw new HyracksDataException("Was not able to find a file in the files index");
        }
    }

    private void setExternalFileFromARecord(ARecord externalFileRecord, ExternalFile file) {
        file.setFileName(((AString) externalFileRecord
                .getValueByPos(FilesIndexDescription.EXTERNAL_FILE_NAME_FIELD_INDEX)).getStringValue());
        file.setSize(((AInt64) externalFileRecord.getValueByPos(FilesIndexDescription.EXTERNAL_FILE_SIZE_FIELD_INDEX))
                .getLongValue());
        file.setLastModefiedTime((new Date(((ADateTime) externalFileRecord
                .getValueByPos(FilesIndexDescription.EXTERNAL_FILE_MOD_DATE_FIELD_INDEX)).getChrononTime())));
    }

    public void closeIndex() throws HyracksDataException {
        try {
            fileIndexSearchCursor.close();
        } finally {
            indexDataflowHelper.close();
        }
    }

}
