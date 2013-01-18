/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.metadata.api;

import java.util.List;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.logging.IndexLogger;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;

/**
 * Descriptor interface for a primary or secondary index on metadata datasets.
 */
public interface IMetadataIndex {
    public String getDataverseName();

    public String getNodeGroupName();

    public String getIndexedDatasetName();

    public List<String> getPartitioningExpr();

    public String getIndexName();

    public int getKeyFieldCount();

    public int getFieldCount();

    public ITypeTraits[] getTypeTraits();

    public RecordDescriptor getRecordDescriptor();

    public IBinaryComparatorFactory[] getKeyBinaryComparatorFactory();

    public IBinaryHashFunctionFactory[] getKeyBinaryHashFunctionFactory();

    public int[] getFieldPermutation();

    public String getFileNameRelativePath();

    public ARecordType getPayloadRecordType();

    public void setFile(FileReference file);

    public FileReference getFile();

    public void setFileId(int fileId);

    public void initIndexLogger(IIndex index) throws ACIDException;

    public int getFileId();

    public void setResourceID(long resourceID);

    public long getResourceID();

    public IndexLogger getIndexLogger();

    public DatasetId getDatasetId();

    boolean isPrimaryIndex();

    int[] getPrimaryKeyIndexes();

}
