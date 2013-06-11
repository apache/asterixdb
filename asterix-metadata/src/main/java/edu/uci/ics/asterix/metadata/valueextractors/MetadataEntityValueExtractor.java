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

package edu.uci.ics.asterix.metadata.valueextractors;

import java.io.IOException;

import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.api.IMetadataEntityTupleTranslator;
import edu.uci.ics.asterix.metadata.api.IValueExtractor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Extracts a metadata entity object from an ITupleReference.
 */
public class MetadataEntityValueExtractor<T> implements IValueExtractor<T> {
    private final IMetadataEntityTupleTranslator<T> tupleReaderWriter;

    public MetadataEntityValueExtractor(IMetadataEntityTupleTranslator<T> tupleReaderWriter) {
        this.tupleReaderWriter = tupleReaderWriter;
    }

    @Override
    public T getValue(JobId jobId, ITupleReference tuple) throws MetadataException, HyracksDataException, IOException {
        return tupleReaderWriter.getMetadataEntytiFromTuple(tuple);
    }
}
