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

import java.io.IOException;
import java.io.Serializable;

import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

/**
 * This Interface represents the component responsible for adding record IDs to tuples when indexing external data
 */
public interface IExternalIndexer extends Serializable {

    /**
     * This method is called by an indexible datasource when the external source reader have been updated.
     * this gives a chance for the indexer to update its reader specific values (i,e. file name)
     *
     * @param reader
     *            the new reader
     * @throws Exception
     */
    public void reset(IIndexingDatasource reader) throws IOException;

    /**
     * This method is called by the dataflow controller with each tuple. the indexer is expected to append record ids to the tuple.
     *
     * @param tb
     * @throws Exception
     */
    public void index(ArrayTupleBuilder tb) throws IOException;

    /**
     * This method returns the number of fields in the record id. It is used by tuple appender at the initialization step.
     *
     * @return
     * @throws Exception
     */
    public int getNumberOfFields() throws IOException;
}
