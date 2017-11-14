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

package org.apache.asterix.metadata.valueextractors;

import java.rmi.RemoteException;

import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.api.IMetadataEntityTupleTranslator;
import org.apache.asterix.metadata.api.IValueExtractor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Extracts a metadata entity object from an ITupleReference.
 */
public class MetadataEntityValueExtractor<T> implements IValueExtractor<T> {
    private final IMetadataEntityTupleTranslator<T> tupleReaderWriter;

    public MetadataEntityValueExtractor(IMetadataEntityTupleTranslator<T> tupleReaderWriter) {
        this.tupleReaderWriter = tupleReaderWriter;
    }

    @Override
    public T getValue(TxnId txnId, ITupleReference tuple)
            throws AlgebricksException, HyracksDataException, RemoteException {
        return tupleReaderWriter.getMetadataEntityFromTuple(tuple);
    }
}
