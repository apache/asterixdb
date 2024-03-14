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
package org.apache.asterix.external.input.record.reader.stream;

import static org.apache.asterix.external.util.ExternalDataConstants.EMPTY_STRING;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_REDACT_WARNINGS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.stream.DiscretizedMultipleInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.IFeedLogManager;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AvroRecordReader extends AbstractStreamRecordReader<GenericRecord> {
    private final org.apache.asterix.external.input.record.GenericRecord<GenericRecord> record;
    private final DiscretizedMultipleInputStream inputStream;
    private final Supplier<String> dataSourceName;
    private GenericRecord avroRecord;
    private DataFileStream<GenericRecord> dataFileStream;
    private boolean done;
    private static final List<String> recordReaderFormats =
            Collections.unmodifiableList(Arrays.asList(ExternalDataConstants.FORMAT_AVRO));

    public AvroRecordReader(AsterixInputStream inputStream, Map<String, String> config) throws IOException {
        record = new org.apache.asterix.external.input.record.GenericRecord<>();
        this.inputStream = new DiscretizedMultipleInputStream(inputStream);
        done = false;
        if (ExternalDataUtils.isTrue(config, KEY_REDACT_WARNINGS)) {
            dataSourceName = EMPTY_STRING;
        } else {
            dataSourceName = inputStream::getStreamName;
        }

        advance();
    }

    @Override
    public void close() throws IOException {
        try {
            if (!done) {
                inputStream.close();
            }
        } finally {
            done = true;
        }
    }

    @Override
    public boolean stop() {
        try {
            inputStream.stop();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public IRawRecord<GenericRecord> next() throws IOException {
        avroRecord = dataFileStream.next(avroRecord);
        record.set(avroRecord);
        return record;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (dataFileStream == null) {
            return false;
        }
        if (dataFileStream.hasNext()) {
            return true;
        }
        return advance() && dataFileStream.hasNext();
    }

    @Override
    public void setFeedLogManager(IFeedLogManager feedLogManager) throws HyracksDataException {
        inputStream.setFeedLogManager(feedLogManager);
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        inputStream.setController(controller);
    }

    @Override
    public boolean handleException(Throwable th) {
        return inputStream.handleException(th);
    }

    @Override
    public List<String> getRecordReaderFormats() {
        return recordReaderFormats;

    }

    @Override
    public void configure(IHyracksTaskContext ctx, AsterixInputStream inputStream, Map<String, String> config)
            throws HyracksDataException {

    }

    private boolean advance() throws IOException {
        try {
            if (inputStream.advance()) {
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                dataFileStream = new DataFileStream<>(inputStream, datumReader);
                return true;
            }
        } catch (InvalidAvroMagicException e) {
            throw new RuntimeDataException(ErrorCode.RECORD_READER_MALFORMED_INPUT_STREAM, e);
        }
        return false;
    }

}
