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
package org.apache.asterix.external.api;

import java.util.List;

import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

/**
 * An interface for external data sources which support indexing
 */
public interface IIndexingDatasource {
    /**
     * @return an external indexer that is used to write RID fields for each record
     */
    public IExternalIndexer getIndexer();

    /**
     * @return a list of external files being accessed
     */
    public List<ExternalFile> getSnapshot();

    /**
     * @return the index of the currently being read file
     */
    public int getCurrentSplitIndex();

    /**
     * @return an HDFS record reader that is used to get the current position in the file
     */
    public RecordReader<?, ? extends Writable> getReader();
}
