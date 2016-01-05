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
package org.apache.asterix.external.dataflow;

import org.apache.asterix.external.api.IExternalIndexer;
import org.apache.asterix.external.api.IIndexingDatasource;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class IndexingDataFlowController<T> extends RecordDataFlowController<T> {
    IExternalIndexer indexer;

    @Override
    protected void appendOtherTupleFields(ArrayTupleBuilder tb) throws Exception {
        indexer.index(tb);
    }

    @Override
    public void setRecordReader(IRecordReader<T> recordReader) throws Exception {
        super.setRecordReader(recordReader);
        indexer = ((IIndexingDatasource) recordReader).getIndexer();
        numOfTupleFields += indexer.getNumberOfFields();
    }
}
