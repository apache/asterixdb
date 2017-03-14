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

package org.apache.asterix.metadata.api;

import java.io.Serializable;
import java.util.List;

import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileReference;

/**
 * Descriptor interface for a primary or secondary index on metadata datasets.
 */
public interface IMetadataIndex extends Serializable {
    public String getDataverseName();

    public String getNodeGroupName();

    public String getIndexedDatasetName();

    public List<List<String>> getPartitioningExpr();

    public List<IAType> getPartitioningExprType();

    public String getIndexName();

    public int getKeyFieldCount();

    public int getFieldCount();

    public ITypeTraits[] getTypeTraits();

    public int[] getBloomFilterKeyFields();

    public RecordDescriptor getRecordDescriptor();

    public IBinaryComparatorFactory[] getKeyBinaryComparatorFactory();

    public IBinaryHashFunctionFactory[] getKeyBinaryHashFunctionFactory();

    public int[] getFieldPermutation();

    public String getFileNameRelativePath();

    public ARecordType getPayloadRecordType();

    public void setFile(FileReference file);

    public FileReference getFile();

    public void setFileId(int fileId);

    public int getFileId();

    public void setResourceId(long resourceId);

    public long getResourceId();

    public DatasetId getDatasetId();

    boolean isPrimaryIndex();

    int[] getPrimaryKeyIndexes();
}
